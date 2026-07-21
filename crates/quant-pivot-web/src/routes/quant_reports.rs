//! Recommendation report HTTP endpoints.
//!
//! # UI integration contract
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/quant/reports` | `quant_report:read` | Paginated report list |
//! | GET | `/quant/reports/current?profile_id&kind` | `quant_report:read` | Current scope authority |
//! | GET | `/quant/reports/{id}` | `quant_report:read` | Report detail (header + summary) |
//! | GET | `/quant/reports/{id}/recommendations` | `quant_report:read` | Report recommendations |
//! | GET | `/quant/reports/{id}/diagnostics` | `quant_report:read` | Durable serving diagnostics |
//! | GET | `/quant/reports/{id}/funnel` | `quant_report:read` | Conserved stage counts |
//! | GET | `/quant/reports/{id}/funnel/markets` | `quant_report:read` | Row-level market decisions |
//! | GET | `/quant/reports/{id}/diff/{other_id}` | `quant_report:read` | Structural diff vs another report |
//! | POST | `/quant/reports/run` | `quant_report:enqueue` (governed) | Enqueue an ad-hoc report (202) |
//! | GET | `/quant/report-runs` | `quant_report:read` | Durable run ledger |
//! | GET | `/quant/report-runs/{id}` | `quant_report:read` | Durable run detail |
//! | POST | `/quant/report-runs/{id}/retry` | `quant_report:enqueue` (governed) | Retry terminal ad-hoc run |
//! | GET | `/quant/report-schedules/health` | `quant_report:read` | Durable scheduler health |
//! | GET | `/quant/report-schedule-gaps` | `quant_report:read` | Append-only schedule gaps |
//! | POST | `/quant/reports/{id}/publication/retry` | `quant_report:enqueue` (governed) | Retry failed delivery |
//! | GET | `/quant/reports/{id}/timeline` | `quant_report:read` | Report-scoped WORM timeline |
//! | POST | `/quant/reports/{id}/revoke` | `quant_report:revoke` (governed) | Revoke a published report |
//!
//! `reports/run` is asynchronous and returns the durable run row (`202` when
//! created, `200` for an idempotent replay). WebSocket events are revision hints;
//! clients always re-fetch durable REST state.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::{
        api::{
            CurrentReportQuery, OperationLogView, QuantRecommendationView, QuantReportDetailView,
            QuantReportDiagnosticsView, QuantReportFunnelView, QuantReportListQuery,
            QuantReportView, ReportDiffView, ReportFactDeliveryView, ReportFunnelMarketListQuery,
            ReportFunnelMarketView, ReportRunListQuery, ReportRunView, ReportScheduleGapListQuery,
            ReportScheduleGapView, ReportScheduleHealthView, ReportTimelineQuery,
            RetryReportRequest, RevokeReportRequest, RunReportRequest,
        },
        pagination::Paginated,
        ports::AdHocReportCommand,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    hashing::CanonicalDigest,
    types::{ContentHash, RecommendationReportId, ReportRunId},
};
use serde::Serialize;

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, RequestId, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Recommendation report routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/quant/reports",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            list,
        ),
        // Declared before `{id}` so the literal segment wins the match.
        spec(
            Method::GET,
            "/quant/reports/current",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            current,
        ),
        spec(
            Method::POST,
            "/quant/reports/run",
            Rule::ActingRoleGoverned(ResourceType::QuantReport, Operation::Enqueue),
            run,
        ),
        spec(
            Method::GET,
            "/quant/report-runs",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            list_runs,
        ),
        spec(
            Method::GET,
            "/quant/report-runs/{id}",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            run_detail,
        ),
        spec(
            Method::POST,
            "/quant/report-runs/{id}/retry",
            Rule::ActingRoleGoverned(ResourceType::QuantReport, Operation::Enqueue),
            retry_run,
        ),
        spec(
            Method::GET,
            "/quant/report-schedules/health",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            schedule_health,
        ),
        spec(
            Method::GET,
            "/quant/report-schedule-gaps",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            schedule_gaps,
        ),
        spec(
            Method::GET,
            "/quant/reports/{id}",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            detail,
        ),
        spec(
            Method::GET,
            "/quant/reports/{id}/recommendations",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            recommendations,
        ),
        spec(
            Method::GET,
            "/quant/reports/{id}/diagnostics",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            diagnostics,
        ),
        spec(
            Method::GET,
            "/quant/reports/{id}/funnel",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            funnel,
        ),
        spec(
            Method::GET,
            "/quant/reports/{id}/funnel/markets",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            funnel_markets,
        ),
        spec(
            Method::GET,
            "/quant/reports/{id}/diff/{other_id}",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            diff,
        ),
        spec(
            Method::POST,
            "/quant/reports/{id}/publication/retry",
            Rule::ActingRoleGoverned(ResourceType::QuantReport, Operation::Enqueue),
            retry_publication,
        ),
        spec(
            Method::GET,
            "/quant/reports/{id}/timeline",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            timeline,
        ),
        spec(
            Method::POST,
            "/quant/reports/{id}/revoke",
            Rule::ActingRoleGoverned(ResourceType::QuantReport, Operation::Revoke),
            revoke,
        ),
    ]
}

/// `GET /api/quant/reports` — paginated, filtered report list.
pub async fn list(
    state: Data<AppState>,
    query: Query<QuantReportListQuery>,
) -> Result<WebResponse<Paginated<QuantReportView>>, WebError> {
    let page = state.quant_reports.list_reports(query.into_inner()).await?;
    Ok(WebResponse::ok(page.map(QuantReportView::from)))
}

/// `GET /api/quant/reports/current` — current authority in one exact scope.
pub async fn current(
    state: Data<AppState>,
    query: Query<CurrentReportQuery>,
) -> Result<WebResponse<QuantReportDetailView>, WebError> {
    let query = query.into_inner();
    if query.profile_id.as_str().trim().is_empty() || query.profile_id.as_str().len() > 128 {
        return Err(WebError::BadRequest(
            "profile_id must contain 1..=128 characters".to_owned(),
        ));
    }
    let info = state
        .quant_reports
        .current_report(&query.profile_id, query.kind)
        .await?
        .ok_or_else(|| WebError::NotFound("no current report for scope".to_owned()))?;
    let delivery = state
        .quant_reports
        .find_report_fact_delivery(&info.recommendation_report_id)
        .await?;
    let run = state
        .quant_reports
        .find_report_run(&info.recommendation_report_id)
        .await?;
    let predecessor = state
        .quant_reports
        .find_report_predecessor_id(&info.recommendation_report_id)
        .await?;
    Ok(WebResponse::ok(QuantReportDetailView::from_parts(
        info,
        delivery,
        run,
        predecessor,
    )))
}

/// `GET /api/quant/reports/{id}` — report detail.
pub async fn detail(
    state: Data<AppState>,
    id: Path<RecommendationReportId>,
) -> Result<WebResponse<QuantReportDetailView>, WebError> {
    let info = state
        .quant_reports
        .find_report(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("report not found: {id}")))?;
    let delivery = state.quant_reports.find_report_fact_delivery(&id).await?;
    let run = state.quant_reports.find_report_run(&id).await?;
    let predecessor = state.quant_reports.find_report_predecessor_id(&id).await?;
    Ok(WebResponse::ok(QuantReportDetailView::from_parts(
        info,
        delivery,
        run,
        predecessor,
    )))
}

/// `GET /api/quant/report-runs` — durable run ledger.
pub async fn list_runs(
    state: Data<AppState>,
    query: Query<ReportRunListQuery>,
) -> Result<WebResponse<Paginated<ReportRunView>>, WebError> {
    let page = state
        .quant_reports
        .list_report_runs(query.into_inner())
        .await?;
    Ok(WebResponse::ok(page.map(ReportRunView::from)))
}

/// `GET /api/quant/report-runs/{id}` — durable run detail and retry lineage.
pub async fn run_detail(
    state: Data<AppState>,
    id: Path<ReportRunId>,
) -> Result<WebResponse<ReportRunView>, WebError> {
    let run = state
        .quant_reports
        .find_report_run_by_id(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("report run not found: {id}")))?;
    Ok(WebResponse::ok(run.into()))
}

/// `POST /api/quant/report-runs/{id}/retry` — retry a terminal ad-hoc run.
pub async fn retry_run(
    state: Data<AppState>,
    id: Path<ReportRunId>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RetryReportRequest>,
) -> Result<WebResponse<ReportRunView>, WebError> {
    let source_run_id = id.into_inner();
    let request = body.into_inner();
    let outcome = state
        .quant_reports
        .retry_report_run(&source_run_id, &request.request_id)
        .await?;
    op_ctx.set_action(OperationCategory::QuantReport, "quant.report_run.retry");
    op_ctx.set_resource(ResourceType::QuantReport, source_run_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "source_run_id": source_run_id,
        "retry_run_id": outcome.run().report_run_id,
        "idempotent_replay": !outcome.created(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "retry_request_id": request.request_id,
        "reason": request.reason,
    }))?;
    let view = ReportRunView::from(outcome.run().clone());
    Ok(if outcome.created() {
        WebResponse::accepted(view)
    } else {
        WebResponse::ok(view)
    })
}

/// `GET /api/quant/report-schedules/health` — durable scheduler health.
pub async fn schedule_health(
    state: Data<AppState>,
) -> Result<WebResponse<ReportScheduleHealthView>, WebError> {
    Ok(WebResponse::ok(
        state.quant_reports.report_schedule_health().await?.into(),
    ))
}

/// `GET /api/quant/report-schedule-gaps` — append-only gap ledger.
pub async fn schedule_gaps(
    state: Data<AppState>,
    query: Query<ReportScheduleGapListQuery>,
) -> Result<WebResponse<Paginated<ReportScheduleGapView>>, WebError> {
    let page = state
        .quant_reports
        .list_report_schedule_gaps(query.into_inner())
        .await?;
    Ok(WebResponse::ok(page.map(ReportScheduleGapView::from)))
}

/// `GET /api/quant/reports/{id}/recommendations` — the report's recommendations.
pub async fn recommendations(
    state: Data<AppState>,
    id: Path<RecommendationReportId>,
) -> Result<WebResponse<Vec<QuantRecommendationView>>, WebError> {
    let views = state.quant_reports.find_recommendations(&id).await?;
    Ok(WebResponse::ok(views))
}

/// `GET /api/quant/reports/{id}/diagnostics` — durable serving evidence summary.
pub async fn diagnostics(
    state: Data<AppState>,
    id: Path<RecommendationReportId>,
) -> Result<WebResponse<QuantReportDiagnosticsView>, WebError> {
    let view = state
        .quant_reports
        .find_report_diagnostics(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("report not found: {id}")))?;
    Ok(WebResponse::ok(view))
}

pub async fn funnel(
    state: Data<AppState>,
    id: Path<RecommendationReportId>,
) -> Result<WebResponse<QuantReportFunnelView>, WebError> {
    let view = state
        .quant_reports
        .find_report_funnel(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("report not found: {id}")))?;
    Ok(WebResponse::ok(view))
}

pub async fn funnel_markets(
    state: Data<AppState>,
    id: Path<RecommendationReportId>,
    query: Query<ReportFunnelMarketListQuery>,
) -> Result<WebResponse<Paginated<ReportFunnelMarketView>>, WebError> {
    let page = state
        .quant_reports
        .page_report_funnel_markets(&id, query.into_inner())
        .await?
        .ok_or_else(|| WebError::NotFound(format!("report not found: {id}")))?;
    Ok(WebResponse::ok(page))
}

/// `GET /api/quant/reports/{id}/diff/{other_id}` — structural diff.
pub async fn diff(
    state: Data<AppState>,
    path: Path<(RecommendationReportId, RecommendationReportId)>,
) -> Result<WebResponse<ReportDiffView>, WebError> {
    let (base, compare) = path.into_inner();
    let diff = state
        .quant_reports
        .diff_reports(&base, &compare)
        .await?
        .ok_or_else(|| WebError::NotFound("report not found for diff".to_owned()))?;
    Ok(WebResponse::ok(ReportDiffView::from(diff)))
}

/// `POST /api/quant/reports/{id}/publication/retry` — requeue failed delivery.
pub async fn retry_publication(
    state: Data<AppState>,
    id: Path<RecommendationReportId>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RetryReportRequest>,
) -> Result<WebResponse<ReportFactDeliveryView>, WebError> {
    let report_id = id.into_inner();
    let request = body.into_inner();
    let delivery = state
        .quant_reports
        .retry_report_publication(&report_id)
        .await?;
    op_ctx.set_action(
        OperationCategory::QuantReport,
        "quant.report.publication.retry",
    );
    op_ctx.set_resource(ResourceType::QuantReport, report_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "report_id": report_id,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "retry_request_id": request.request_id,
        "reason": request.reason,
    }))?;
    Ok(WebResponse::accepted(delivery.into()))
}

/// `GET /api/quant/reports/{id}/timeline` — report-scoped WORM projection.
pub async fn timeline(
    state: Data<AppState>,
    id: Path<RecommendationReportId>,
    query: Query<ReportTimelineQuery>,
) -> Result<WebResponse<Paginated<OperationLogView>>, WebError> {
    let report_id = id.into_inner();
    let page = state
        .quant_reports
        .report_timeline(&report_id, query.into_inner())
        .await?
        .ok_or_else(|| WebError::NotFound(format!("report not found: {report_id}")))?;
    Ok(WebResponse::ok(page.map(OperationLogView::from)))
}

/// `POST /api/quant/reports/run` — enqueue an ad-hoc report build (202).
pub async fn run(
    state: Data<AppState>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RunReportRequest>,
) -> Result<WebResponse<ReportRunView>, WebError> {
    // Gate at the boundary on the hot config snapshot (the builder re-checks the
    // point-in-time config as defense in depth).
    if !state
        .runtime_config_apply
        .current()
        .recommendation
        .reports
        .ad_hoc_report_enabled
    {
        return Err(WebError::Conflict(
            "ad-hoc report generation is disabled".to_owned(),
        ));
    }
    let request = body.into_inner();
    let reason = request.reason.clone();
    let outcome = state
        .quant_reports
        .enqueue_ad_hoc(AdHocReportCommand {
            request_id: request.request_id,
            top_n: request.top_n,
            knowledge_lag_secs: request.knowledge_lag_secs,
        })
        .await?;
    op_ctx.set_action(OperationCategory::QuantReport, "quant.report.run");
    op_ctx.set_resource(
        ResourceType::QuantReport,
        outcome.run().report_run_id.to_string(),
    );
    op_ctx.set_detail(serde_json::json!({
        "request_id": outcome.run().request_id,
        "trigger_key": outcome.run().trigger_key,
        "report_run_id": outcome.run().report_run_id,
        "idempotent_replay": !outcome.created(),
        "acting_role": acting_role.0,
        "correlation_request_id": request_id.0,
        "reason": reason,
    }))?;
    let created = outcome.created();
    let view = ReportRunView::from(outcome.run().clone());
    Ok(if created {
        WebResponse::accepted(view)
    } else {
        WebResponse::ok(view)
    })
}

/// `POST /api/quant/reports/{id}/revoke` — revoke a published report.
pub async fn revoke(
    state: Data<AppState>,
    id: Path<RecommendationReportId>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RevokeReportRequest>,
) -> Result<WebResponse<QuantReportDetailView>, WebError> {
    let request = body.into_inner();
    let report_id = id.into_inner();
    let before = state
        .quant_reports
        .find_report(&report_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("report not found: {report_id}")))?;
    let before_hash = canonical_state_hash(&before)?;
    let report = state
        .quant_reports
        .revoke(&report_id, &request.reason)
        .await?;
    let after_hash = canonical_state_hash(&report)?;
    op_ctx.set_action(OperationCategory::QuantReport, "quant.report.revoke");
    op_ctx.set_resource(
        ResourceType::QuantReport,
        report.recommendation_report_id.to_string(),
    );
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "report_id": report.recommendation_report_id.to_string(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }))?;
    Ok(WebResponse::ok(QuantReportDetailView::from(report)))
}

fn canonical_state_hash<T: Serialize>(state: &T) -> Result<ContentHash, WebError> {
    CanonicalDigest::content_hash_json(state)
        .map_err(|error| WebError::Internal(format!("canonical state hash failed: {error}")))
}
