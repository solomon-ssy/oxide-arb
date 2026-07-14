//! Recommendation + evidence + attribution HTTP endpoints (Phase 04.4 / 05.7).
//!
//! # UI integration contract
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET  | `/quant/recommendations/{id}` | `quant_report:read` | One recommendation |
//! | GET  | `/quant/recommendations/{id}/evidence` | `quant_report:read` | Replay handles |
//! | GET  | `/quant/recommendations/{id}/entry-condition` | `quant_report:read` | Durable condition state and tree |
//! | GET  | `/quant/recommendations/{id}/entry-condition/audits` | `quant_report:read` | WORM condition timeline |
//! | GET  | `/quant/recommendations/{id}/attribution` | `recommendation_attribution:read` | Final WORM attribution |
//!
//! Creating an order intent from a recommendation is `POST /api/quant/intents`
//! (see [`super::quant_intents`]), the governed execution surface added in
//! Phase 05.2.

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        EntryConditionArtifactView, EntryConditionAuditView, EntryConditionDetailView,
        EntryConditionInstanceSummaryView, QuantEvidenceView, QuantRecommendationView,
        RecommendationAttributionView,
    },
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
        spec(
            Method::GET,
            "/quant/recommendations/{id}/entry-condition",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            entry_condition,
        ),
        spec(
            Method::GET,
            "/quant/recommendations/{id}/entry-condition/audits",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            entry_condition_audits,
        ),
    ]
}

/// Durable recommendation-owned condition state and immutable artifact.
pub async fn entry_condition(
    state: web::Data<AppState>,
    id: web::Path<RecommendationId>,
) -> Result<WebResponse<EntryConditionDetailView>, WebError> {
    let instance = state
        .entry_conditions
        .find_by_recommendation(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("entry condition not found: {id}")))?;
    let artifact = match instance.artifact_id.as_ref() {
        Some(artifact_id) => {
            let info = state
                .entry_conditions
                .find_artifact(artifact_id)
                .await?
                .ok_or_else(|| {
                    WebError::Internal(format!(
                        "condition instance {} references missing artifact {artifact_id}",
                        instance.condition_instance_id
                    ))
                })?;
            if instance.artifact_hash.as_ref() != Some(&info.content_hash) {
                return Err(WebError::Internal(format!(
                    "condition instance {} artifact hash mismatch",
                    instance.condition_instance_id
                )));
            }
            let nodes = info
                .payload_json
                .root
                .preorder_nodes()
                .map_err(|error| WebError::Internal(error.to_string()))?;
            Some(EntryConditionArtifactView::from_info(info, nodes))
        }
        None => None,
    };
    Ok(WebResponse::ok(EntryConditionDetailView {
        instance: EntryConditionInstanceSummaryView::from(instance),
        artifact,
    }))
}

/// WORM condition lifecycle timeline ordered by revision.
pub async fn entry_condition_audits(
    state: web::Data<AppState>,
    id: web::Path<RecommendationId>,
) -> Result<WebResponse<Vec<EntryConditionAuditView>>, WebError> {
    let instance = state
        .entry_conditions
        .find_by_recommendation(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("entry condition not found: {id}")))?;
    let audits = state
        .entry_conditions
        .audits(&instance.condition_instance_id)
        .await?
        .into_iter()
        .map(EntryConditionAuditView::from)
        .collect();
    Ok(WebResponse::ok(audits))
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
