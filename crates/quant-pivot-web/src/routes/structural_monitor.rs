//! Neg-risk structural-drift monitor endpoint (Phase 11.2.1).
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/quant/structural/negrisk-events` | `quant_report:read` | Live per-event leg-sum drift |

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::NegRiskEventDriftView,
    enums::rbac::{Operation, ResourceType},
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Structural-monitor routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![spec(
        Method::GET,
        "/quant/structural/negrisk-events",
        Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
        negrisk_events,
    )]
}

/// `GET /api/quant/structural/negrisk-events` — live neg-risk leg-sum drift,
/// most-mispriced first.
pub async fn negrisk_events(
    state: web::Data<AppState>,
) -> Result<WebResponse<Vec<NegRiskEventDriftView>>, WebError> {
    let events = state.structural_monitor.negrisk_events().await?;
    Ok(WebResponse::ok(events))
}
