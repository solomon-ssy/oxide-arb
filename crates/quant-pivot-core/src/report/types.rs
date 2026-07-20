//! Shared report module DTOs.

use chrono::{DateTime, Utc};
use quant_pivot_error::QuantError;
use quant_pivot_models::{
    clickhouse::{QuantReportRecommendationFactRow, ReportMarketFunnelRow},
    domain::NewReportTransaction,
    enums::quant::{
        EmptyReportReason, OutcomeSide, QuantRuntimeMode, ReportKind, ReportTriggerKind,
    },
    runtime_config::ReportDeliveryPolicy,
    types::{CorrelationId, Probability, RecommendationReportId, ReportScheduleId, Usd},
};

/// Source that triggered one report build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReportTrigger {
    /// Scheduled generation by schedule id.
    Scheduled { schedule_id: ReportScheduleId },
    /// Operator/API requested ad-hoc generation by stable request id.
    AdHoc { request_id: CorrelationId },
}

impl ReportTrigger {
    /// Stable trigger kind persisted on the report header.
    #[must_use]
    pub const fn kind(&self) -> ReportTriggerKind {
        match self {
            Self::Scheduled { .. } => ReportTriggerKind::Scheduled,
            Self::AdHoc { .. } => ReportTriggerKind::AdHoc,
        }
    }

    /// Fixed idempotency key contract.
    #[must_use]
    pub fn key(&self, trigger_time: DateTime<Utc>) -> String {
        match self {
            Self::Scheduled { schedule_id } => {
                format!("scheduled:{schedule_id}:{}", trigger_time.to_rfc3339())
            }
            Self::AdHoc { request_id } => format!("ad_hoc:{request_id}"),
        }
    }
}

/// Builder input after lifecycle idempotency resolution.
#[derive(Debug, Clone)]
pub struct BuildReportRequest {
    pub trigger: ReportTrigger,
    pub trigger_time: DateTime<Utc>,
    pub top_n_override: Option<u32>,
    pub knowledge_lag_secs_override: Option<u64>,
}

/// Context carried when a report is intentionally empty.
#[derive(Debug, Clone)]
pub struct EmptyReportContext {
    pub reason: EmptyReportReason,
    pub candidate_count: u32,
    pub rejected_count: u32,
    pub warnings: Vec<String>,
}

/// One recommendation summarized for an operator notification (`TopN` preview).
#[derive(Debug, Clone)]
pub struct NotificationRecommendation {
    pub market_id: String,
    pub outcome_side: OutcomeSide,
    pub score: Probability,
    pub suggested_usd: Option<Usd>,
}

/// Operator-facing notification payload for a committed report.
#[derive(Debug, Clone)]
pub struct ReportNotificationPayload {
    pub report_id: RecommendationReportId,
    pub kind: ReportKind,
    pub status: String,
    pub runtime_mode: QuantRuntimeMode,
    pub published_count: u32,
    pub total_suggested_usd: Usd,
    pub top3: Vec<NotificationRecommendation>,
    pub warnings: Vec<String>,
    pub empty_reason: Option<EmptyReportReason>,
}

/// Complete report artifact ready for atomic PG write and post-commit publish.
#[derive(Debug, Clone)]
pub struct ComposedReport {
    pub transaction: NewReportTransaction,
    pub ch_rows: Vec<QuantReportRecommendationFactRow>,
    pub funnel_rows: Vec<ReportMarketFunnelRow>,
    pub notification: ReportNotificationPayload,
    pub delivery_policy: ReportDeliveryPolicy,
    pub notify_operators: bool,
}

/// Stable operator-facing summary for durable/API error projections.
///
/// Raw dependency diagnostics may contain credentials, signed URLs, query
/// parameters, or host paths. They remain in correlated structured logs and
/// must never be copied into `PostgreSQL` rows returned by the report API.
pub(super) fn durable_report_error_summary(error: &QuantError) -> String {
    format!(
        "{} failure; inspect correlated structured logs",
        error.code()
    )
}

#[cfg(test)]
mod tests {
    use quant_pivot_error::{QuantError, report::ReportError};

    use super::durable_report_error_summary;

    #[test]
    fn durable_error_summary_does_not_persist_raw_diagnostic() {
        let error = QuantError::from(ReportError::InvariantViolation {
            stage: "test",
            detail: "secret-token=must-not-persist".to_owned(),
        });

        let summary = durable_report_error_summary(&error);
        assert_eq!(
            summary,
            "report failure; inspect correlated structured logs"
        );
        assert!(!summary.contains("must-not-persist"));
    }
}
