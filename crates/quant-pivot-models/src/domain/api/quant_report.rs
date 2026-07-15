//! Quant recommendation report HTTP contract types.
//!
//! Three families live here per the DTO paradigm: outbound `*View` projections
//! (`Serialize`-only), the inbound `QuantReportListQuery` (paginated filter), and
//! the governed mutation requests `RunReportRequest` / `RevokeReportRequest`
//! (`Deserialize` + `Validate`). Views are built from persistence `*Info` / the
//! computed `ReportDiff`; the persistence structs are never serialized directly.

use crate::{
    domain::{
        DecisionBoundaryEvidenceView, ModelRouteEvidenceView, RecommendationDelta,
        RecommendationReportInfo, ReportDiff, ReportFactDeliveryInfo, pagination::PageRequest,
    },
    enums::quant::{
        AccountSource, EmptyReportReason, FeatureParityStage, OutcomeSide, QuantRuntimeMode,
        RecommendationReportStatus, ReportFactDeliveryStatus, ReportKind, ReportTriggerKind,
    },
    types::{
        AccountSnapshotId, ContentHash, EligibilitySummary, EventId, FeatureVectorId, MarketId,
        MarketSelectionId, ModelRunId, ModelVersionId, RecommendationId, RecommendationReportId,
        ReportFunnelReason, ReportFunnelStage, ReportSummary, ResearchProfileRef,
        RuntimeConfigVersionId, SignalCandidateId, TokenId, Usd,
    },
};
use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use validator::Validate;

/// List-row projection of a recommendation report (header + summary roll-up).
#[derive(Debug, Clone, Serialize)]
pub struct QuantReportView {
    pub recommendation_report_id: RecommendationReportId,
    pub profile_ref: ResearchProfileRef,
    pub report_kind: ReportKind,
    pub trigger_kind: ReportTriggerKind,
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
    pub revoked_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub status_reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl From<RecommendationReportInfo> for QuantReportView {
    fn from(info: RecommendationReportInfo) -> Self {
        Self {
            recommendation_report_id: info.recommendation_report_id,
            profile_ref: info.profile_ref,
            report_kind: info.report_kind,
            trigger_kind: info.trigger_kind,
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
    pub profile_ref: ResearchProfileRef,
    pub report_kind: ReportKind,
    pub trigger_kind: ReportTriggerKind,
    pub trigger_key: String,
    pub trigger_time: DateTime<Utc>,
    pub knowledge_lag_secs: i64,
    pub decision_at: DateTime<Utc>,
    pub horizon_secs: i64,
    pub runtime_mode: QuantRuntimeMode,
    pub top_n: i32,
    pub status: RecommendationReportStatus,
    pub account_source: AccountSource,
    pub capital_base_usd: Usd,
    pub account_snapshot_ref: AccountSnapshotId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub model_run_id: Option<ModelRunId>,
    pub model_version_id: ModelVersionId,
    pub market_selection_id: MarketSelectionId,
    pub summary: ReportSummary,
    pub fact_delivery: Option<ReportFactDeliveryView>,
    pub published_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub expired_at: Option<DateTime<Utc>>,
    pub status_reason: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl From<RecommendationReportInfo> for QuantReportDetailView {
    fn from(info: RecommendationReportInfo) -> Self {
        Self {
            recommendation_report_id: info.recommendation_report_id,
            profile_ref: info.profile_ref,
            report_kind: info.report_kind,
            trigger_kind: info.trigger_kind,
            trigger_key: info.trigger_key,
            trigger_time: info.trigger_time,
            knowledge_lag_secs: info.knowledge_lag_secs,
            decision_at: info.decision_at,
            horizon_secs: info.horizon_secs,
            runtime_mode: info.runtime_mode,
            top_n: info.top_n,
            status: info.status,
            account_source: info.account_source,
            capital_base_usd: info.capital_base_usd,
            account_snapshot_ref: info.account_snapshot_ref,
            runtime_config_version_id: info.runtime_config_version_id,
            model_run_id: info.model_run_id,
            model_version_id: info.model_version_id,
            market_selection_id: info.market_selection_id,
            summary: info.summary_json,
            fact_delivery: None,
            published_at: info.published_at,
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
    ) -> Self {
        let mut view = Self::from(info);
        view.fact_delivery = delivery.map(Into::into);
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
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: Option<ModelRunId>,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub terminal_stage: ReportFunnelStage,
    pub primary_reason: ReportFunnelReason,
    pub secondary_diagnostics: serde_json::Value,
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
    pub kind: Option<ReportKind>,
    pub status: Option<RecommendationReportStatus>,
    pub trigger_kind: Option<ReportTriggerKind>,
    pub runtime_mode: Option<QuantRuntimeMode>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
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

/// Accepted-response body for an enqueued ad-hoc report.
///
/// The report id does not exist until the build commits, so the async enqueue
/// returns the idempotency key and the derived trigger key as correlation
/// handles. Clients track completion via the `quant.report` WebSocket channel
/// (`started` → `published`/`empty`/`failed`) or by listing reports.
#[derive(Debug, Clone, Serialize)]
pub struct RunReportAccepted {
    pub request_id: String,
    pub trigger_key: String,
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
    pub base_recommendation_id: Option<RecommendationId>,
    pub compare_recommendation_id: Option<RecommendationId>,
    pub base_rank: Option<i32>,
    pub compare_rank: Option<i32>,
    pub base_suggested_usd: Option<Usd>,
    pub compare_suggested_usd: Option<Usd>,
    pub suggested_usd_delta: Usd,
}

impl From<RecommendationDelta> for RecommendationDeltaView {
    fn from(delta: RecommendationDelta) -> Self {
        Self {
            market_id: delta.market_id.to_string(),
            outcome_side: delta.outcome_side,
            base_recommendation_id: delta.base_recommendation_id,
            compare_recommendation_id: delta.compare_recommendation_id,
            base_rank: delta.base_rank,
            compare_rank: delta.compare_rank,
            base_suggested_usd: delta.base_suggested_usd,
            compare_suggested_usd: delta.compare_suggested_usd,
            suggested_usd_delta: delta.suggested_usd_delta,
        }
    }
}
