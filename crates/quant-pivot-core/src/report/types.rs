//! Shared report module DTOs.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    clickhouse::QuantRecommendationEventRow,
    domain::NewReportTransaction,
    enums::quant::{
        EmptyReportReason, OutcomeSide, QuantRuntimeMode, ReportKind, ReportTriggerKind,
    },
    runtime_config::ReportDeliveryPolicy,
    types::{Probability, RecommendationReportId, Usd},
};

/// Source that triggered one report build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReportTrigger {
    /// Scheduled generation by schedule id.
    Scheduled { schedule_id: String },
    /// Operator/API requested ad-hoc generation by stable request id.
    AdHoc { request_id: String },
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
    pub suggested_usd: Usd,
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
    pub ch_rows: Vec<QuantRecommendationEventRow>,
    pub notification: ReportNotificationPayload,
    pub delivery_policy: ReportDeliveryPolicy,
    pub notify_operators: bool,
}
