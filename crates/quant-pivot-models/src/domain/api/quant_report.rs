//! Quant recommendation report HTTP contract types.
//!
//! Three families live here per the DTO paradigm: outbound `*View` projections
//! (`Serialize`-only), the inbound `QuantReportListQuery` (paginated filter), and
//! the governed mutation requests `RunReportRequest` / `RevokeReportRequest`
//! (`Deserialize` + `Validate`). Views are built from persistence `*Info` / the
//! computed `ReportDiff`; the persistence structs are never serialized directly.

use crate::{
    domain::{RecommendationDelta, RecommendationReportInfo, ReportDiff, pagination::PageRequest},
    enums::quant::{
        AccountSource, EmptyReason, OutcomeSide, QuantRuntimeMode, RecommendationReportStatus,
        ReportKind, ReportTriggerKind,
    },
    types::{
        AccountSnapshotId, EligibilitySummary, MarketSelectionId, ModelVersionId, RecommendationId,
        RecommendationReportId, ReportSummary, RuntimeConfigVersionId, Usd,
    },
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// List-row projection of a recommendation report (header + summary roll-up).
#[derive(Debug, Clone, Serialize)]
pub struct QuantReportView {
    pub recommendation_report_id: RecommendationReportId,
    pub report_kind: ReportKind,
    pub trigger_kind: ReportTriggerKind,
    pub status: RecommendationReportStatus,
    pub runtime_mode: QuantRuntimeMode,
    pub as_of: DateTime<Utc>,
    pub top_n: i32,
    pub account_source: AccountSource,
    pub capital_base_usd: Usd,
    pub published_recommendation_count: u32,
    pub total_suggested_usd: Usd,
    pub empty_reason: Option<EmptyReason>,
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
            report_kind: info.report_kind,
            trigger_kind: info.trigger_kind,
            status: info.status,
            runtime_mode: info.runtime_mode,
            as_of: info.as_of,
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
    pub report_kind: ReportKind,
    pub trigger_kind: ReportTriggerKind,
    pub trigger_key: String,
    pub trigger_time: DateTime<Utc>,
    pub source_delay_secs: i64,
    pub as_of: DateTime<Utc>,
    pub horizon_secs: i64,
    pub runtime_mode: QuantRuntimeMode,
    pub top_n: i32,
    pub status: RecommendationReportStatus,
    pub account_source: AccountSource,
    pub capital_base_usd: Usd,
    pub account_snapshot_ref: AccountSnapshotId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub model_version_id: ModelVersionId,
    pub market_selection_id: MarketSelectionId,
    pub summary: ReportSummary,
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
            report_kind: info.report_kind,
            trigger_kind: info.trigger_kind,
            trigger_key: info.trigger_key,
            trigger_time: info.trigger_time,
            source_delay_secs: info.source_delay_secs,
            as_of: info.as_of,
            horizon_secs: info.horizon_secs,
            runtime_mode: info.runtime_mode,
            top_n: info.top_n,
            status: info.status,
            account_source: info.account_source,
            capital_base_usd: info.capital_base_usd,
            account_snapshot_ref: info.account_snapshot_ref,
            runtime_config_version_id: info.runtime_config_version_id,
            model_version_id: info.model_version_id,
            market_selection_id: info.market_selection_id,
            summary: info.summary_json,
            published_at: info.published_at,
            revoked_at: info.revoked_at,
            expired_at: info.expired_at,
            status_reason: info.status_reason,
            created_at: info.created_at,
        }
    }
}

/// Paginated filter for listing recommendation reports.
///
/// `from` / `to` bound `created_at`; the pagination window is the shared
/// [`PageRequest`], flattened so the query string stays flat.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct QuantReportListQuery {
    pub kind: Option<ReportKind>,
    pub status: Option<RecommendationReportStatus>,
    pub trigger_kind: Option<ReportTriggerKind>,
    pub runtime_mode: Option<QuantRuntimeMode>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub page: PageRequest,
}

impl QuantReportListQuery {
    /// Return a copy with the embedded pagination window normalized.
    #[must_use]
    pub const fn normalized(self) -> Self {
        Self {
            page: self.page.normalized(),
            ..self
        }
    }
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
    pub source_delay_secs: Option<u64>,
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
