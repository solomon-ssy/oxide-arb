//! Recommendation + evidence + attribution HTTP endpoints (Phase 04.4 / 05.7).
//!
//! # UI integration contract
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET  | `/quant/recommendations/{id}` | `quant_report:read` | One recommendation |
//! | GET  | `/quant/recommendations/{id}/evidence` | `quant_report:read` | Replay handles |
//! | GET  | `/quant/recommendations/{id}/attribution` | `recommendation_attribution:read` | Final WORM attribution |
//!
//! Creating an order intent from a recommendation is `POST /api/quant/intents`
//! (see [`super::quant_intents`]), the governed execution surface added in
//! Phase 05.2.

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{QuantEvidenceView, QuantRecommendationView, RecommendationAttributionView},
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
            Method::GET,
            "/quant/recommendations/{id}/attribution",
            Rule::ResourceOp(ResourceType::RecommendationAttribution, Operation::Read),
            attribution,
        ),
    ]
}

/// `GET /api/quant/recommendations/{id}` — one recommendation.
pub async fn get(
    state: web::Data<AppState>,
    id: web::Path<RecommendationId>,
) -> Result<WebResponse<QuantRecommendationView>, WebError> {
    let view = state
        .quant_reports
        .find_recommendation(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("recommendation not found: {id}")))?;
    Ok(WebResponse::ok(view))
}

/// `GET /api/quant/recommendations/{id}/evidence` — replay handles.
pub async fn evidence(
    state: web::Data<AppState>,
    id: web::Path<RecommendationId>,
) -> Result<WebResponse<QuantEvidenceView>, WebError> {
    let view = state
        .quant_reports
        .find_evidence(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("recommendation not found: {id}")))?;
    Ok(WebResponse::ok(view))
}

/// `GET /api/quant/recommendations/{id}/attribution` — final WORM attribution.
pub async fn attribution(
    state: web::Data<AppState>,
    id: web::Path<RecommendationId>,
) -> Result<WebResponse<RecommendationAttributionView>, WebError> {
    let info = state
        .execution_read
        .get_recommendation_attribution(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("recommendation attribution not found: {id}")))?;
    Ok(WebResponse::ok(RecommendationAttributionView::from(info)))
}
