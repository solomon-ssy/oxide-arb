//! Recommendation report HTTP endpoints (Phase 04.4).
//!
//! # UI integration contract
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET  | `/quant/reports` | `quant_report:read` | Paginated report list |
//! | GET  | `/quant/reports/latest` | `quant_report:read` | Latest published `TopN` report |
//! | GET  | `/quant/reports/{id}` | `quant_report:read` | Report detail (header + summary) |
//! | GET  | `/quant/reports/{id}/recommendations` | `quant_report:read` | Report recommendations |
//! | GET  | `/quant/reports/{id}/diff/{other_id}` | `quant_report:read` | Structural diff vs another report |
//! | POST | `/quant/reports/run` | `quant_report:enqueue` (governed) | Enqueue an ad-hoc report (202) |
//! | POST | `/quant/reports/{id}/revoke` | `quant_report:revoke` (governed) | Revoke a published report |
//!
//! `reports/run` is **asynchronous**: it enqueues the build and returns `202` with
//! the idempotency key + derived trigger key. The report id does not exist until
//! the build commits; clients track completion via the `quant.report` WebSocket
//! channel (`started` → `published`/`empty`/`failed`) or by listing reports.

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        AdHocReportCommand, Paginated, QuantRecommendationView, QuantReportDetailView,
        QuantReportListQuery, QuantReportView, ReportDiffView, RevokeReportRequest,
        RunReportAccepted, RunReportRequest,
    },
    enums::{
        operation_log::OperationCategory,
        quant::ReportKind,
        rbac::{Operation, ResourceType},
    },
    hashing::CanonicalDigest,
    types::RecommendationReportId,
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
            "/quant/reports/latest",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            latest,
        ),
        spec(
            Method::POST,
            "/quant/reports/run",
            Rule::ActingRoleGoverned(ResourceType::QuantReport, Operation::Enqueue),
            run,
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
            "/quant/reports/{id}/diff/{other_id}",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            diff,
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
    state: web::Data<AppState>,
    query: web::Query<QuantReportListQuery>,
) -> Result<WebResponse<Paginated<QuantReportView>>, WebError> {
    let page = state
        .quant_reports
        .list_reports(query.into_inner().normalized())
        .await?;
    Ok(WebResponse::ok(page.map(QuantReportView::from)))
}

/// `GET /api/quant/reports/latest` — latest published `TopN` report.
pub async fn latest(
    state: web::Data<AppState>,
) -> Result<WebResponse<QuantReportDetailView>, WebError> {
    let info = state
        .quant_reports
        .latest_report(ReportKind::TopN)
        .await?
        .ok_or_else(|| WebError::NotFound("no published report".to_owned()))?;
    Ok(WebResponse::ok(QuantReportDetailView::from(info)))
}

/// `GET /api/quant/reports/{id}` — report detail.
pub async fn detail(
    state: web::Data<AppState>,
    id: web::Path<RecommendationReportId>,
) -> Result<WebResponse<QuantReportDetailView>, WebError> {
    let info = state
        .quant_reports
        .find_report(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("report not found: {id}")))?;
    Ok(WebResponse::ok(QuantReportDetailView::from(info)))
}

/// `GET /api/quant/reports/{id}/recommendations` — the report's recommendations.
pub async fn recommendations(
    state: web::Data<AppState>,
    id: web::Path<RecommendationReportId>,
) -> Result<WebResponse<Vec<QuantRecommendationView>>, WebError> {
    let recs = state.quant_reports.find_recommendations(&id).await?;
    let views = recs
        .into_iter()
        .map(QuantRecommendationView::from)
        .collect();
    Ok(WebResponse::ok(views))
}

/// `GET /api/quant/reports/{id}/diff/{other_id}` — structural diff.
pub async fn diff(
    state: web::Data<AppState>,
    path: web::Path<(RecommendationReportId, RecommendationReportId)>,
) -> Result<WebResponse<ReportDiffView>, WebError> {
    let (base, compare) = path.into_inner();
    let diff = state
        .quant_reports
        .diff_reports(&base, &compare)
        .await?
        .ok_or_else(|| WebError::NotFound("report not found for diff".to_owned()))?;
    Ok(WebResponse::ok(ReportDiffView::from(diff)))
}

/// `POST /api/quant/reports/run` — enqueue an ad-hoc report build (202).
pub async fn run(
    state: web::Data<AppState>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RunReportRequest>,
) -> Result<WebResponse<RunReportAccepted>, WebError> {
    // Gate at the boundary on the hot config snapshot (the builder re-checks the
    // point-in-time config as defense in depth).
    if !state
        .runtime_config_apply
        .current()
        .reports
        .ad_hoc_report_enabled
    {
        return Err(WebError::Conflict(
            "ad-hoc report generation is disabled".to_owned(),
        ));
    }
    let request = body.into_inner();
    let reason = request.reason.clone();
    let enqueued = state
        .quant_reports
        .enqueue_ad_hoc(AdHocReportCommand {
            request_id: request.request_id,
            top_n: request.top_n,
            source_delay_secs: request.source_delay_secs,
        })
        .await?;
    op_ctx.set_action(OperationCategory::QuantReport, "quant.report.run");
    op_ctx.set_resource(ResourceType::QuantReport, enqueued.trigger_key.clone());
    op_ctx.set_detail(serde_json::json!({
        "request_id": enqueued.request_id,
        "trigger_key": enqueued.trigger_key,
        "acting_role": acting_role.0,
        "correlation_request_id": request_id.0,
        "reason": reason,
    }));
    Ok(WebResponse::accepted(RunReportAccepted {
        request_id: enqueued.request_id,
        trigger_key: enqueued.trigger_key,
    }))
}

/// `POST /api/quant/reports/{id}/revoke` — revoke a published report.
pub async fn revoke(
    state: web::Data<AppState>,
    id: web::Path<RecommendationReportId>,
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
    }));
    Ok(WebResponse::ok(QuantReportDetailView::from(report)))
}

fn canonical_state_hash<T: Serialize>(state: &T) -> Result<String, WebError> {
    CanonicalDigest::content_hash_json(state)
        .map(|hash| hash.as_str().to_owned())
        .map_err(|error| WebError::Internal(format!("canonical state hash failed: {error}")))
}
