//! Web-facing port for the quant report read + governed-mutation surface.
//!
//! This is the dependency-inversion boundary between the HTTP handlers and the
//! core report services (`ReportLifecycleService` + `ReportScheduleRunner` + the
//! read repositories). Handlers depend only on this trait — never on a
//! repository or a venue client directly. Implemented in `quant-pivot-core` and
//! injected into `quant_pivot_web::AppState`.

use async_trait::async_trait;

use crate::{
    domain::{
        Paginated, QuantEvidenceView, QuantRecommendationView, QuantReportDiagnosticsView,
        QuantReportListQuery, RecommendationReportInfo, ReportDiff,
    },
    enums::quant::ReportKind,
    types::{RecommendationId, RecommendationReportId},
};
use quant_pivot_error::QuantResult;

/// Validated command to enqueue an ad-hoc report build.
///
/// The decision time is assigned by the port implementation at enqueue. The
/// knowledge lag only derives source cutoffs and never shifts the decision time,
/// so the caller supplies neither clock value.
#[derive(Debug, Clone)]
pub struct AdHocReportCommand {
    /// Caller-supplied idempotency key (`ad_hoc:{request_id}` trigger key).
    pub request_id: String,
    /// Optional override of the configured `TopN` width.
    pub top_n: Option<u32>,
    /// Optional override of the configured PIT knowledge lag.
    pub knowledge_lag_secs: Option<u64>,
}

/// Outcome of enqueuing an ad-hoc report: correlation handles for the async job.
///
/// The report id does not exist until the build commits, so the response carries
/// the idempotency key and the derived trigger key; the client observes
/// completion via the `quant.report` WebSocket channel or by listing reports.
#[derive(Debug, Clone)]
pub struct AdHocReportEnqueued {
    pub request_id: String,
    pub trigger_key: String,
}

/// Read + governed-mutation port for recommendation reports.
#[async_trait]
pub trait QuantReportPort: Send + Sync {
    /// Page reports filtered by kind / status / trigger / `created_at` window.
    async fn list_reports(
        &self,
        query: QuantReportListQuery,
    ) -> QuantResult<Paginated<RecommendationReportInfo>>;

    /// Load one report by id.
    async fn find_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<Option<RecommendationReportInfo>>;

    /// Load durable serving diagnostics for one report.
    async fn find_report_diagnostics(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<Option<QuantReportDiagnosticsView>>;

    /// Load the latest published (or published-empty) report of `kind`.
    async fn latest_report(
        &self,
        kind: ReportKind,
    ) -> QuantResult<Option<RecommendationReportInfo>>;

    /// Load all recommendations for a report as fully-assembled views (ranked),
    /// with parent report status and any blocking intent resolved.
    async fn find_recommendations(
        &self,
        report_id: &RecommendationReportId,
    ) -> QuantResult<Vec<QuantRecommendationView>>;

    /// Load one recommendation by id as a fully-assembled view (parent report
    /// status + blocking intent resolved). Returns `None` when it does not exist.
    async fn find_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<Option<QuantRecommendationView>>;

    /// Load one recommendation's replay-handle evidence. Returns `None` when the
    /// recommendation does not exist.
    async fn find_evidence(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<Option<QuantEvidenceView>>;

    /// Compute the structural diff between two reports.
    ///
    /// Returns `None` when either report does not exist.
    async fn diff_reports(
        &self,
        base_report_id: &RecommendationReportId,
        compare_report_id: &RecommendationReportId,
    ) -> QuantResult<Option<ReportDiff>>;

    /// Enqueue an ad-hoc report build (async; does not block on the pipeline).
    async fn enqueue_ad_hoc(&self, command: AdHocReportCommand)
    -> QuantResult<AdHocReportEnqueued>;

    /// Revoke a published report, recording `reason` and emitting the lifecycle
    /// event. Returns the updated report row.
    async fn revoke(
        &self,
        report_id: &RecommendationReportId,
        reason: &str,
    ) -> QuantResult<RecommendationReportInfo>;
}
