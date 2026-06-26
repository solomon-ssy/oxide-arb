//! Recommendation + evidence HTTP endpoints (Phase 04.4).
//!
//! # UI integration contract
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET  | `/quant/recommendations/{id}` | `quant_report:read` | One recommendation |
//! | GET  | `/quant/recommendations/{id}/evidence` | `quant_report:read` | Replay handles |
//! | POST | `/quant/recommendations/{id}/create-intent` | `quant_report:read` | **501** — execution lands in Phase 5 |
//!
//! `create-intent` is forward-declared so a client is never misled by a silent
//! `404` or a fake success: it returns `501 Not Implemented` with an explicit
//! message. Execution (intent creation / admission / submit) lands in Phase 5.

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{QuantEvidenceView, QuantRecommendationView},
    enums::rbac::{Operation, ResourceType},
    types::RecommendationId,
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Recommendation + evidence routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/quant/recommendations/{id}",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            get,
        ),
        spec(
            Method::GET,
            "/quant/recommendations/{id}/evidence",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            evidence,
        ),
        spec(
            Method::POST,
            "/quant/recommendations/{id}/create-intent",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            create_intent,
        ),
    ]
}

/// `GET /api/quant/recommendations/{id}` — one recommendation.
pub async fn get(
    state: web::Data<AppState>,
    id: web::Path<RecommendationId>,
) -> Result<WebResponse<QuantRecommendationView>, WebError> {
    let info = state
        .quant_reports
        .find_recommendation(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("recommendation not found: {id}")))?;
    Ok(WebResponse::ok(QuantRecommendationView::from(info)))
}

/// `GET /api/quant/recommendations/{id}/evidence` — replay handles.
pub async fn evidence(
    state: web::Data<AppState>,
    id: web::Path<RecommendationId>,
) -> Result<WebResponse<QuantEvidenceView>, WebError> {
    let info = state
        .quant_reports
        .find_recommendation(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("recommendation not found: {id}")))?;
    Ok(WebResponse::ok(QuantEvidenceView::from(info)))
}

/// `POST /api/quant/recommendations/{id}/create-intent` — Phase 5 placeholder.
///
/// Returns `501` so execution is never silently dropped or faked; the intent /
/// admission / submit flow lands in Phase 5.
pub async fn create_intent(
    _state: web::Data<AppState>,
    _id: web::Path<RecommendationId>,
) -> Result<WebResponse<()>, WebError> {
    Err(WebError::NotImplemented(
        "order execution is available in Phase 5".to_owned(),
    ))
}
