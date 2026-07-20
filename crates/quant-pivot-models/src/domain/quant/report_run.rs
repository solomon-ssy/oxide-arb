//! Durable report-run and schedule-coordinator contracts.

use crate::{
    entities::{
        quant_recommendation_report, quant_report_run, quant_report_schedule_gap,
        quant_report_schedule_state,
    },
    enums::quant::{
        ReportKind, ReportRunStatus, ReportRunTerminalReason, ReportScheduleGapReason,
        ReportTriggerKind,
    },
    types::{
        ContentHash, CorrelationId, DecisionPolicySnapshotId, DiagnosticCode,
        RecommendationReportId, ReportRunId, ReportScheduleGapId, ReportScheduleId,
        ResearchProfileId, WorkerId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

/// Full durable projection of one report build attempt.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_report_run::Entity")]
pub struct ReportRunInfo {
    pub report_run_id: ReportRunId,
    pub trigger_kind: ReportTriggerKind,
    pub trigger_key: String,
    pub schedule_id: Option<ReportScheduleId>,
    pub request_id: Option<CorrelationId>,
    pub retry_of_run_id: Option<ReportRunId>,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub requested_at: DateTime<Utc>,
    pub status: ReportRunStatus,
    pub started_at: Option<DateTime<Utc>>,
    pub decision_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub lease_owner: Option<WorkerId>,
    pub decision_policy_snapshot_id: Option<DecisionPolicySnapshotId>,
    pub top_n: Option<i32>,
    pub knowledge_lag_secs: Option<i64>,
    pub output_report_id: Option<RecommendationReportId>,
    pub terminal_reason: Option<ReportRunTerminalReason>,
    pub error_code: Option<DiagnosticCode>,
    pub error_summary: Option<String>,
}

info_from_model!(ReportRunInfo, quant_report_run::Model, {
    report_run_id,
    trigger_kind,
    trigger_key,
    schedule_id,
    request_id,
    retry_of_run_id,
    scheduled_for,
    requested_at,
    status,
    started_at,
    decision_at,
    heartbeat_at,
    lease_expires_at,
    finished_at,
    lease_owner,
    decision_policy_snapshot_id,
    top_n,
    knowledge_lag_secs,
    output_report_id,
    terminal_reason,
    error_code,
    error_summary,
});

/// Insert payload for a queued report run.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_report_run::ActiveModel")]
pub struct NewReportRun {
    pub report_run_id: ReportRunId,
    pub trigger_kind: ReportTriggerKind,
    pub trigger_key: String,
    pub schedule_id: Option<ReportScheduleId>,
    pub request_id: Option<CorrelationId>,
    pub retry_of_run_id: Option<ReportRunId>,
    pub scheduled_for: Option<DateTime<Utc>>,
    pub requested_at: DateTime<Utc>,
    pub status: ReportRunStatus,
    pub top_n: Option<i32>,
    pub knowledge_lag_secs: Option<i64>,
}

/// Lease identity required by every running-build CAS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportRunClaim {
    pub report_run_id: ReportRunId,
    pub lease_owner: WorkerId,
    pub lease_expires_at: DateTime<Utc>,
}

/// Idempotent enqueue result.
#[derive(Debug, Clone)]
pub enum EnqueueReportRunOutcome {
    Created(ReportRunInfo),
    Existing(ReportRunInfo),
}

impl EnqueueReportRunOutcome {
    #[must_use]
    pub const fn created(&self) -> bool {
        matches!(self, Self::Created(_))
    }

    #[must_use]
    pub const fn run(&self) -> &ReportRunInfo {
        match self {
            Self::Created(run) | Self::Existing(run) => run,
        }
    }
}

/// Durable derived state for one configured report schedule.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_report_schedule_state::Entity")]
pub struct ReportScheduleStateInfo {
    pub schedule_id: ReportScheduleId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub spec_hash: ContentHash,
    pub next_scheduled_for: DateTime<Utc>,
    pub last_materialized_for: Option<DateTime<Utc>>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(ReportScheduleStateInfo, quant_report_schedule_state::Model, {
    schedule_id,
    decision_policy_snapshot_id,
    spec_hash,
    next_scheduled_for,
    last_materialized_for,
    enabled,
    created_at,
    updated_at,
});

/// Append-only aggregate of contiguous missed occurrences.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_report_schedule_gap::Entity")]
pub struct ReportScheduleGapInfo {
    pub gap_id: ReportScheduleGapId,
    pub schedule_id: ReportScheduleId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub reason: ReportScheduleGapReason,
    pub first_scheduled_for: DateTime<Utc>,
    pub last_scheduled_for: DateTime<Utc>,
    pub missed_count: i64,
    pub detected_at: DateTime<Utc>,
    pub detail: Option<String>,
}

/// Point-in-time operational health snapshot calculated from durable rows.
#[derive(Debug, Clone)]
pub struct ReportScheduleHealthInfo {
    pub observed_at: DateTime<Utc>,
    pub active_run: Option<ReportRunInfo>,
    pub queued_run_count: u64,
    pub failed_run_count_24h: u64,
    pub gap_count_24h: u64,
    pub missed_occurrence_count_24h: i64,
    pub prepared_report_count: u64,
    pub current_reports: Vec<ReportCurrentHealthInfo>,
    pub schedules: Vec<ReportScheduleStateInfo>,
}

/// One scope's current report authority in the durable health projection.
#[derive(Debug, Clone, sea_orm::FromQueryResult)]
pub struct ReportCurrentHealthInfo {
    pub recommendation_report_id: RecommendationReportId,
    pub profile_id: ResearchProfileId,
    pub report_kind: ReportKind,
    pub published_at: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
}

impl From<quant_recommendation_report::Model> for ReportCurrentHealthInfo {
    fn from(model: quant_recommendation_report::Model) -> Self {
        Self {
            recommendation_report_id: model.recommendation_report_id,
            profile_id: model.research_profile_artifact_id.profile_ref().id,
            report_kind: model.report_kind,
            published_at: model.published_at,
            valid_until: model.valid_until,
        }
    }
}

/// One active runtime-config schedule spec prepared for durable reconciliation.
#[derive(Debug, Clone)]
pub struct ReconcileReportSchedule {
    pub schedule_id: ReportScheduleId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub spec_hash: ContentHash,
    pub next_scheduled_for: DateTime<Utc>,
    pub enabled: bool,
}

/// Atomic latest-only materialization request for one due schedule cursor.
#[derive(Debug, Clone)]
pub struct MaterializeReportSchedule {
    pub schedule_id: ReportScheduleId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub spec_hash: ContentHash,
    pub expected_next_scheduled_for: DateTime<Utc>,
    pub latest_scheduled_for: DateTime<Utc>,
    pub next_scheduled_for: DateTime<Utc>,
    pub earlier_first_scheduled_for: Option<DateTime<Utc>>,
    pub earlier_last_scheduled_for: Option<DateTime<Utc>>,
    pub earlier_missed_count: i64,
}

/// Active config inputs resolved by the claim transaction for one schedule.
#[derive(Debug, Clone)]
pub struct ClaimReportSchedule {
    pub schedule_id: ReportScheduleId,
    pub top_n: i32,
    pub knowledge_lag_secs: i64,
}

/// Exact active-config defaults supplied to the claim transaction.
#[derive(Debug, Clone)]
pub struct ReportRunClaimConfig {
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub ad_hoc_default_top_n: i32,
    pub ad_hoc_default_knowledge_lag_secs: i64,
    pub schedules: Vec<ClaimReportSchedule>,
}

/// Rows changed by one schedule-state reconcile transaction.
#[derive(Debug, Clone, Default)]
pub struct ReconcileReportSchedulesOutcome {
    pub states: Vec<ReportScheduleStateInfo>,
    pub skipped_runs: Vec<ReportRunInfo>,
    pub gaps: Vec<ReportScheduleGapInfo>,
}

/// Rows committed by one latest-only schedule materialization transaction.
#[derive(Debug, Clone)]
pub struct MaterializeReportScheduleOutcome {
    pub run: ReportRunInfo,
    pub skipped_run: Option<ReportRunInfo>,
    pub gaps: Vec<ReportScheduleGapInfo>,
    pub state: ReportScheduleStateInfo,
}

info_from_model!(ReportScheduleGapInfo, quant_report_schedule_gap::Model, {
    gap_id,
    schedule_id,
    decision_policy_snapshot_id,
    reason,
    first_scheduled_for,
    last_scheduled_for,
    missed_count,
    detected_at,
    detail,
});

#[cfg(test)]
mod tests {
    use crate::enums::quant::{RecommendationReportStatus, RecommendationStatus, ReportRunStatus};

    #[test]
    fn prepared_report_is_not_actionable_before_fact_verification() {
        assert!(!RecommendationReportStatus::Prepared.is_current_authority());
        assert!(!RecommendationStatus::Prepared.allows_new_intent());
        assert!(RecommendationReportStatus::Published.is_current_authority());
        assert!(RecommendationStatus::Published.allows_new_intent());
    }

    #[test]
    fn report_lifecycle_transition_table_is_closed() {
        let run_states = [
            ReportRunStatus::Queued,
            ReportRunStatus::Running,
            ReportRunStatus::Succeeded,
            ReportRunStatus::Failed,
            ReportRunStatus::Skipped,
            ReportRunStatus::Abandoned,
        ];
        let report_states = [
            RecommendationReportStatus::Prepared,
            RecommendationReportStatus::Published,
            RecommendationReportStatus::Superseded,
            RecommendationReportStatus::Obsolete,
            RecommendationReportStatus::Revoked,
            RecommendationReportStatus::Expired,
        ];

        for from in run_states {
            for to in run_states {
                let expected = matches!(
                    (from, to),
                    (
                        ReportRunStatus::Queued,
                        ReportRunStatus::Running | ReportRunStatus::Skipped
                    ) | (
                        ReportRunStatus::Running,
                        ReportRunStatus::Succeeded
                            | ReportRunStatus::Failed
                            | ReportRunStatus::Abandoned
                    )
                );
                assert_eq!(
                    from.allows_transition_to(to),
                    expected,
                    "{from:?} -> {to:?}"
                );
            }
        }
        for from in report_states {
            for to in report_states {
                let expected = matches!(
                    (from, to),
                    (
                        RecommendationReportStatus::Prepared,
                        RecommendationReportStatus::Published
                            | RecommendationReportStatus::Obsolete
                            | RecommendationReportStatus::Revoked
                    ) | (
                        RecommendationReportStatus::Published,
                        RecommendationReportStatus::Superseded
                            | RecommendationReportStatus::Revoked
                            | RecommendationReportStatus::Expired
                    )
                );
                assert_eq!(
                    from.allows_transition_to(to),
                    expected,
                    "{from:?} -> {to:?}"
                );
            }
        }
    }
}
