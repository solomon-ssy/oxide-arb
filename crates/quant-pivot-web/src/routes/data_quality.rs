//! Data-quality observability endpoint (Phase 2).

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::DataQualitySnapshot,
    enums::rbac::{Operation, ResourceType},
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![spec(
        Method::GET,
        "/quant/data-quality",
        Rule::ResourceOp(ResourceType::System, Operation::Read),
        snapshot,
    )]
}

/// Aggregate live book-plane data-quality classification.
pub async fn snapshot(
    state: web::Data<AppState>,
) -> Result<WebResponse<DataQualitySnapshot>, WebError> {
    Ok(WebResponse::ok(state.data_quality.snapshot()))
}
