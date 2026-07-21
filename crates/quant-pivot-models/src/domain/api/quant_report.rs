//! Quant recommendation report HTTP contract types.
//!
//! Three families live here per the DTO paradigm: outbound `*View` projections
//! (`Serialize`-only), the inbound `QuantReportListQuery` (paginated filter), and
//! the governed mutation requests `RunReportRequest` / `RevokeReportRequest`
//! (`Deserialize` + `Validate`). Views are built from persistence `*Info` / the
//! computed `ReportDiff`; the persistence structs are never serialized directly.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{
        api::{DecisionBoundaryEvidenceView, ModelRouteEvidenceView},
        pagination::PageRequest,
        quant::{
            RecommendationChangedField, RecommendationDelta, RecommendationDiffSnapshot,
            RecommendationReportInfo, ReportCurrentHealthInfo, ReportDiff, ReportFactDeliveryInfo,
            ReportRunInfo, ReportScheduleGapInfo, ReportScheduleHealthInfo,
            ReportScheduleStateInfo,
        },
    },
    enums::quant::{
        AccountSource, EmptyReportReason, FeatureParityStage, OutcomeSide, QuantRuntimeMode,
        RecommendationReportStatus, ReportFactDeliveryStatus, ReportKind, ReportRunStatus,
        ReportRunTerminalReason, ReportScheduleGapReason, ReportTriggerKind,
    },
    types::{
        AccountSnapshotId, Bps, ContentHash, CorrelationId, DecisionPolicySnapshotId,
        DiagnosticCode, EligibilitySummary, EventId, ExecutionEligibility, FeatureVectorId,
        MarketId, MarketSelectionId, ModelRunId, ModelVersionId, Probability,
        RecommendationFactorBreakdown, RecommendationId, RecommendationReportId,
        RecommendationTradePlan, ReportFunnelDiagnostics, ReportFunnelReason, ReportFunnelStage,
        ReportRunId, ReportScheduleGapId, ReportScheduleId, ReportSummary, ReportTriggerKey,
        ResearchProfileId, ResearchProfileRef, SignalCandidateId, TokenId, Usd, WorkerId,
    },
};

/// List-row projection of a recommendation report (header + summary roll-up).
#[derive(Debug, Clone, Serialize)]
pub struct QuantReportView {
    pub recommendation_report_id: RecommendationReportId,
    pub profile_id: ResearchProfileId,
    pub profile_ref: ResearchProfileRef,
    pub report_kind: ReportKind,
    pub status: RecommendationReportStatus,
    pub runtime_mode: QuantRuntimeMode,
    pub decision_at: DateTime<Utc>,
    pub top_n: i32,
    pub account_source: AccountSource,
    pub capital_base_usd: Usd,
    pub published_recommendation_count: u32,
    pub total_suggested_usd: Usd,
    pub empty_reason: Option<EmptyReportReason>,
    pub published_at: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub successor_report_id: Option<RecommendationReportId>,
    pub superseded_at: Option<DateTime<Utc>>,
    pub obsoleted_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub status_reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl From<RecommendationReportInfo> for QuantReportView {
    fn from(info: RecommendationReportInfo) -> Self {
        Self {
            recommendation_report_id: info.recommendation_report_id,
            profile_id: info.profile_id,
            profile_ref: info.profile_ref,
            report_kind: info.report_kind,
            status: info.status,
            runtime_mode: info.runtime_mode,
            decision_at: info.decision_at,
            top_n: info.top_n,
            account_source: info.account_source,
            capital_base_usd: info.capital_base_usd,
            published_recommendation_count: info.summary_json.published_recommendation_count,
            total_suggested_usd: info.summary_json.total_suggested_usd,
            empty_reason: info.summary_json.empty_reason,
            published_at: info.published_at,
            valid_until: info.valid_until,
            successor_report_id: info.successor_report_id,
            superseded_at: info.superseded_at,
            obsoleted_at: info.obsoleted_at,
            revoked_at: info.revoked_at,
            expired_at: info.expired_at,
            status_reason: info.status_reason,
            created_at: info.created_at,
        }
    }
}

/// Full report header projection: lifecycle + account base + replay handles +
/// the report-level [`ReportSummary`].
#[derive(Debug, Clone, Serialize)]
pub struct QuantReportDetailView {
    pub recommendation_report_id: RecommendationReportId,
    pub profile_id: ResearchProfileId,
    pub profile_ref: ResearchProfileRef,
    pub report_kind: ReportKind,
    pub decision_at: DateTime<Utc>,
    pub horizon_secs: i64,
    pub runtime_mode: QuantRuntimeMode,
    pub top_n: i32,
    pub status: RecommendationReportStatus,
    pub account_source: AccountSource,
    pub capital_base_usd: Usd,
    pub account_snapshot_ref: AccountSnapshotId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_run_id: Option<ModelRunId>,
    pub model_version_id: ModelVersionId,
    pub market_selection_id: MarketSelectionId,
    pub summary: ReportSummary,
    pub fact_delivery: Option<ReportFactDeliveryView>,
    pub run: Option<ReportRunView>,
    pub published_at: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
    pub successor_report_id: Option<RecommendationReportId>,
    pub predecessor_report_id: Option<RecommendationReportId>,
    pub superseded_at: Option<DateTime<Utc>>,
    pub obsoleted_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub status_reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl From<RecommendationReportInfo> for QuantReportDetailView {
    fn from(info: RecommendationReportInfo) -> Self {
        Self {
            recommendation_report_id: info.recommendation_report_id,
            profile_id: info.profile_id,
            profile_ref: info.profile_ref,
            report_kind: info.report_kind,
            decision_at: info.decision_at,
            horizon_secs: info.horizon_secs,
            runtime_mode: info.runtime_mode,
            top_n: info.top_n,
            status: info.status,
            account_source: info.account_source,
            capital_base_usd: info.capital_base_usd,
            account_snapshot_ref: info.account_snapshot_ref,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            model_run_id: info.model_run_id,
            model_version_id: info.model_version_id,
            market_selection_id: info.market_selection_id,
            summary: info.summary_json,
            fact_delivery: None,
            run: None,
            published_at: info.published_at,
            valid_until: info.valid_until,
            successor_report_id: info.successor_report_id,
            predecessor_report_id: None,
            superseded_at: info.superseded_at,
            obsoleted_at: info.obsoleted_at,
            revoked_at: info.revoked_at,
            expired_at: info.expired_at,
            status_reason: info.status_reason,
            created_at: info.created_at,
        }
    }
}

impl QuantReportDetailView {
    #[must_use]
    pub fn from_parts(
        info: RecommendationReportInfo,
        delivery: Option<ReportFactDeliveryInfo>,
        run: Option<ReportRunInfo>,
        predecessor_report_id: Option<RecommendationReportId>,
    ) -> Self {
        let mut view = Self::from(info);
        view.fact_delivery = delivery.map(Into::into);
        view.run = run.map(Into::into);
        view.predecessor_report_id = predecessor_report_id;
        view
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportFactDeliveryView {
    pub status: ReportFactDeliveryStatus,
    pub bundle_hash: ContentHash,
    pub recommendation_row_count: i64,
    pub recommendation_row_chain_hash: ContentHash,
    pub funnel_row_count: i64,
    pub funnel_row_chain_hash: ContentHash,
    pub attempt_count: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub verified_at: Option<DateTime<Utc>>,
    pub announced_at: Option<DateTime<Utc>>,
}

impl From<ReportFactDeliveryInfo> for ReportFactDeliveryView {
    fn from(info: ReportFactDeliveryInfo) -> Self {
        Self {
            status: info.status,
            bundle_hash: info.bundle_hash,
            recommendation_row_count: info.recommendation_row_count,
            recommendation_row_chain_hash: info.recommendation_row_chain_hash,
            funnel_row_count: info.funnel_row_count,
            funnel_row_chain_hash: info.funnel_row_chain_hash,
            attempt_count: info.attempt_count,
            next_attempt_at: info.next_attempt_at,
            last_error: info.last_error,
            verified_at: info.verified_at,
            announced_at: info.announced_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportFunnelStageView {
    pub stage: ReportFunnelStage,
    pub input_count: u64,
    pub output_count: u64,
    pub excluded_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QuantReportFunnelView {
    pub recommendation_report_id: RecommendationReportId,
    pub catalog_visible_count: u64,
    pub published_count: u64,
    pub conserved: bool,
    pub stages: Vec<ReportFunnelStageView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportFunnelMarketView {
    pub recommendation_report_id: RecommendationReportId,
    pub market_selection_id: MarketSelectionId,
    pub profile_ref: ResearchProfileRef,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: Option<ModelRunId>,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub terminal_stage: ReportFunnelStage,
    pub primary_reason: ReportFunnelReason,
    pub secondary_diagnostics: ReportFunnelDiagnostics,
    pub feature_vector_id: Option<FeatureVectorId>,
    pub signal_candidate_id: Option<SignalCandidateId>,
    pub recommendation_id: Option<RecommendationId>,
    pub row_hash: ContentHash,
}

#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct ReportFunnelMarketListQuery {
    pub terminal_stage: Option<ReportFunnelStage>,
    pub primary_reason: Option<ReportFunnelReason>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Subject whose durable evidence is projected by report diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportDiagnosticsSubject {
    /// A real serving model run; the report stores its exact `model_run_id`.
    ModelRun,
    /// A committed report that intentionally stopped before model inference.
    PreInferenceReport,
}

/// Durable serving diagnostics through the last stage actually executed.
#[derive(Debug, Clone, Serialize)]
pub struct QuantReportDiagnosticsView {
    pub subject: ReportDiagnosticsSubject,
    pub stage_ceiling: FeatureParityStage,
    pub evidence_complete: bool,
    pub decision_boundary: Option<DecisionBoundaryEvidenceView>,
    pub model_route: Option<ModelRouteEvidenceView>,
    /// Selection is committed for every report, including an empty selection.
    pub selection_count: u64,
    /// `None` means the feature/capture stage was not executed or its evidence
    /// is unavailable; it is never encoded as a synthetic zero.
    pub decision_capture_count: Option<u64>,
    pub feature_vector_count: Option<u64>,
    pub feature_state_counts: Option<BTreeMap<String, u64>>,
    pub feature_cell_count: Option<u64>,
    /// `None` is the only valid representation before model inference or when
    /// serving-input evidence is unavailable.
    pub model_input_state_counts: Option<BTreeMap<String, u64>>,
    pub model_input_count: Option<u64>,
}

/// Paginated filter for listing recommendation reports.
///
/// `from` / `to` bound the report `decision_at`; the pagination window is the shared
/// [`PageRequest`], flattened so the query string stays flat.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct QuantReportListQuery {
    pub profile_id: Option<ResearchProfileId>,
    pub kind: Option<ReportKind>,
    pub status: Option<RecommendationReportStatus>,
    pub runtime_mode: Option<QuantRuntimeMode>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Required scope of the unique current-report authority.
#[derive(Debug, Clone, Deserialize)]
pub struct CurrentReportQuery {
    pub profile_id: ResearchProfileId,
    pub kind: ReportKind,
}

/// Inbound body for `POST /quant/reports/run` (ad-hoc report generation).
///
/// `request_id` is the caller-supplied idempotency key (the scheduler derives the
/// `ad_hoc:{request_id}` trigger key); `reason` is recorded in the operation log.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RunReportRequest {
    #[validate(length(min = 1, max = 128))]
    pub request_id: String,
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
    #[validate(range(min = 1, max = 500))]
    pub top_n: Option<u32>,
    pub knowledge_lag_secs: Option<u64>,
}

/// Inbound body for `POST /quant/reports/{id}/revoke`.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RevokeReportRequest {
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Governed retry command for a terminal ad-hoc run or failed publication.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RetryReportRequest {
    #[validate(length(min = 1, max = 128))]
    pub request_id: String,
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Durable report-run projection returned by enqueue, list, detail, and retry.
#[derive(Debug, Clone, Serialize)]
pub struct ReportRunView {
    pub report_run_id: ReportRunId,
    pub trigger_kind: ReportTriggerKind,
    pub trigger_key: ReportTriggerKey,
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
    pub lease_owner: Option<WorkerId>,
    pub finished_at: Option<DateTime<Utc>>,
    pub decision_policy_snapshot_id: Option<DecisionPolicySnapshotId>,
    pub top_n: Option<i32>,
    pub knowledge_lag_secs: Option<i64>,
    pub output_report_id: Option<RecommendationReportId>,
    pub terminal_reason: Option<ReportRunTerminalReason>,
    pub error_code: Option<DiagnosticCode>,
    pub error_summary: Option<String>,
}

impl From<ReportRunInfo> for ReportRunView {
    fn from(info: ReportRunInfo) -> Self {
        Self {
            report_run_id: info.report_run_id,
            trigger_kind: info.trigger_kind,
            trigger_key: info.trigger_key,
            schedule_id: info.schedule_id,
            request_id: info.request_id,
            retry_of_run_id: info.retry_of_run_id,
            scheduled_for: info.scheduled_for,
            requested_at: info.requested_at,
            status: info.status,
            started_at: info.started_at,
            decision_at: info.decision_at,
            heartbeat_at: info.heartbeat_at,
            lease_expires_at: info.lease_expires_at,
            lease_owner: info.lease_owner,
            finished_at: info.finished_at,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            top_n: info.top_n,
            knowledge_lag_secs: info.knowledge_lag_secs,
            output_report_id: info.output_report_id,
            terminal_reason: info.terminal_reason,
            error_code: info.error_code,
            error_summary: info.error_summary,
        }
    }
}

/// Paginated report-run filters.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct ReportRunListQuery {
    pub status: Option<ReportRunStatus>,
    pub trigger_kind: Option<ReportTriggerKind>,
    pub schedule_id: Option<ReportScheduleId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Paginated append-only schedule-gap filters.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct ReportScheduleGapListQuery {
    pub schedule_id: Option<ReportScheduleId>,
    pub reason: Option<ReportScheduleGapReason>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Paginated report-scoped operation timeline query.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct ReportTimelineQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// One durable schedule-gap row.
#[derive(Debug, Clone, Serialize)]
pub struct ReportScheduleGapView {
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

impl From<ReportScheduleGapInfo> for ReportScheduleGapView {
    fn from(info: ReportScheduleGapInfo) -> Self {
        Self {
            gap_id: info.gap_id,
            schedule_id: info.schedule_id,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            reason: info.reason,
            first_scheduled_for: info.first_scheduled_for,
            last_scheduled_for: info.last_scheduled_for,
            missed_count: info.missed_count,
            detected_at: info.detected_at,
            detail: info.detail,
        }
    }
}

/// Durable schedule cursor exposed in the scheduler-health projection.
#[derive(Debug, Clone, Serialize)]
pub struct ReportScheduleStateView {
    pub schedule_id: ReportScheduleId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub spec_hash: ContentHash,
    pub next_scheduled_for: DateTime<Utc>,
    pub last_materialized_for: Option<DateTime<Utc>>,
    pub enabled: bool,
    pub updated_at: DateTime<Utc>,
}

impl From<ReportScheduleStateInfo> for ReportScheduleStateView {
    fn from(info: ReportScheduleStateInfo) -> Self {
        Self {
            schedule_id: info.schedule_id,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            spec_hash: info.spec_hash,
            next_scheduled_for: info.next_scheduled_for,
            last_materialized_for: info.last_materialized_for,
            enabled: info.enabled,
            updated_at: info.updated_at,
        }
    }
}

/// Operational health derived from `PostgreSQL` rather than process memory.
#[derive(Debug, Clone, Serialize)]
pub struct ReportScheduleHealthView {
    pub observed_at: DateTime<Utc>,
    pub active_run: Option<ReportRunView>,
    pub queued_run_count: u64,
    pub failed_run_count_24h: u64,
    pub gap_count_24h: u64,
    pub missed_occurrence_count_24h: i64,
    pub prepared_report_count: u64,
    pub current_reports: Vec<ReportCurrentHealthView>,
    pub schedules: Vec<ReportScheduleStateView>,
}

/// One scope's current report authority in scheduler health.
#[derive(Debug, Clone, Serialize)]
pub struct ReportCurrentHealthView {
    pub recommendation_report_id: RecommendationReportId,
    pub profile_id: ResearchProfileId,
    pub report_kind: ReportKind,
    pub published_at: Option<DateTime<Utc>>,
    pub valid_until: Option<DateTime<Utc>>,
}

impl From<ReportCurrentHealthInfo> for ReportCurrentHealthView {
    fn from(info: ReportCurrentHealthInfo) -> Self {
        Self {
            recommendation_report_id: info.recommendation_report_id,
            profile_id: info.profile_id,
            report_kind: info.report_kind,
            published_at: info.published_at,
            valid_until: info.valid_until,
        }
    }
}

impl From<ReportScheduleHealthInfo> for ReportScheduleHealthView {
    fn from(info: ReportScheduleHealthInfo) -> Self {
        Self {
            observed_at: info.observed_at,
            active_run: info.active_run.map(Into::into),
            queued_run_count: info.queued_run_count,
            failed_run_count_24h: info.failed_run_count_24h,
            gap_count_24h: info.gap_count_24h,
            missed_occurrence_count_24h: info.missed_occurrence_count_24h,
            prepared_report_count: info.prepared_report_count,
            current_reports: info.current_reports.into_iter().map(Into::into).collect(),
            schedules: info.schedules.into_iter().map(Into::into).collect(),
        }
    }
}

/// Outbound projection of a [`ReportDiff`].
#[derive(Debug, Clone, Serialize)]
pub struct ReportDiffView {
    pub base_report_id: RecommendationReportId,
    pub compare_report_id: RecommendationReportId,
    pub added: Vec<RecommendationDeltaView>,
    pub removed: Vec<RecommendationDeltaView>,
    pub retained: Vec<RecommendationDeltaView>,
    pub base_total_suggested_usd: Usd,
    pub compare_total_suggested_usd: Usd,
    pub total_suggested_usd_delta: Usd,
    pub base_eligibility: EligibilitySummary,
    pub compare_eligibility: EligibilitySummary,
}

impl From<ReportDiff> for ReportDiffView {
    fn from(diff: ReportDiff) -> Self {
        Self {
            base_report_id: diff.base_report_id,
            compare_report_id: diff.compare_report_id,
            added: diff
                .added
                .into_iter()
                .map(RecommendationDeltaView::from)
                .collect(),
            removed: diff
                .removed
                .into_iter()
                .map(RecommendationDeltaView::from)
                .collect(),
            retained: diff
                .retained
                .into_iter()
                .map(RecommendationDeltaView::from)
                .collect(),
            base_total_suggested_usd: diff.base_total_suggested_usd,
            compare_total_suggested_usd: diff.compare_total_suggested_usd,
            total_suggested_usd_delta: diff.total_suggested_usd_delta,
            base_eligibility: diff.eligibility.base,
            compare_eligibility: diff.eligibility.compare,
        }
    }
}

/// Outbound projection of a single `(market, side)` recommendation delta.
#[derive(Debug, Clone, Serialize)]
pub struct RecommendationDeltaView {
    pub market_id: String,
    pub outcome_side: OutcomeSide,
    pub base: Option<RecommendationDiffSnapshotView>,
    pub compare: Option<RecommendationDiffSnapshotView>,
    pub changed_fields: Vec<RecommendationChangedFieldView>,
    pub suggested_usd_delta: Usd,
}

impl From<RecommendationDelta> for RecommendationDeltaView {
    fn from(delta: RecommendationDelta) -> Self {
        Self {
            market_id: delta.market_id.to_string(),
            outcome_side: delta.outcome_side,
            base: delta.base.map(Into::into),
            compare: delta.compare.map(Into::into),
            changed_fields: delta.changed_fields.into_iter().map(Into::into).collect(),
            suggested_usd_delta: delta.suggested_usd_delta,
        }
    }
}

/// Typed decision snapshot for one side of a recommendation diff.
#[derive(Debug, Clone, Serialize)]
pub struct RecommendationDiffSnapshotView {
    pub recommendation_id: RecommendationId,
    pub rank: i32,
    pub composite_score: Probability,
    pub risk_adjusted_score: Probability,
    pub confidence: Probability,
    pub expected_return_bps: Bps,
    pub downside_bps: Bps,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub execution_eligibility: ExecutionEligibility,
    pub trade_plan: RecommendationTradePlan,
    pub factor_breakdown: RecommendationFactorBreakdown,
}

impl From<RecommendationDiffSnapshot> for RecommendationDiffSnapshotView {
    fn from(snapshot: RecommendationDiffSnapshot) -> Self {
        Self {
            recommendation_id: snapshot.recommendation_id,
            rank: snapshot.rank,
            composite_score: snapshot.composite_score,
            risk_adjusted_score: snapshot.risk_adjusted_score,
            confidence: snapshot.confidence,
            expected_return_bps: snapshot.expected_return_bps,
            downside_bps: snapshot.downside_bps,
            valid_from: snapshot.valid_from,
            valid_until: snapshot.valid_until,
            execution_eligibility: snapshot.execution_eligibility,
            trade_plan: snapshot.trade_plan,
            factor_breakdown: snapshot.factor_breakdown,
        }
    }
}

/// Stable field vocabulary used to group diff details without raw JSON.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecommendationChangedFieldView {
    Rank,
    CompositeScore,
    RiskAdjustedScore,
    Confidence,
    ExpectedReturn,
    Downside,
    Sizing,
    Validity,
    Eligibility,
    TradePlanAvailability,
    Entry,
    Exit,
    FactorBreakdown,
}

impl From<RecommendationChangedField> for RecommendationChangedFieldView {
    fn from(field: RecommendationChangedField) -> Self {
        match field {
            RecommendationChangedField::Rank => Self::Rank,
            RecommendationChangedField::CompositeScore => Self::CompositeScore,
            RecommendationChangedField::RiskAdjustedScore => Self::RiskAdjustedScore,
            RecommendationChangedField::Confidence => Self::Confidence,
            RecommendationChangedField::ExpectedReturn => Self::ExpectedReturn,
            RecommendationChangedField::Downside => Self::Downside,
            RecommendationChangedField::Sizing => Self::Sizing,
            RecommendationChangedField::Validity => Self::Validity,
            RecommendationChangedField::Eligibility => Self::Eligibility,
            RecommendationChangedField::TradePlanAvailability => Self::TradePlanAvailability,
            RecommendationChangedField::Entry => Self::Entry,
            RecommendationChangedField::Exit => Self::Exit,
            RecommendationChangedField::FactorBreakdown => Self::FactorBreakdown,
        }
    }
}
