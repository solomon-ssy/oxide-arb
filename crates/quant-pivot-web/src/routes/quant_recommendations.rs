//! Recommendation + evidence HTTP endpoints (Phase 04.4).
//!
//! # UI integration contract
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET  | `/quant/recommendations/{id}` | `quant_report:read` | One recommendation |
//! | GET  | `/quant/recommendations/{id}/evidence` | `quant_report:read` | Replay handles |
//!
//! Creating an order intent from a recommendation is `POST /api/quant/intents`
//! (see [`super::quant_intents`]), the governed execution surface added in
//! Phase 05.2.

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
