//! Data-quality observability endpoint.

use actix_web::{http::Method, web::Data};
use quant_pivot_models::{
    domain::data_plane::DataQualitySnapshot,
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
pub async fn snapshot(state: Data<AppState>) -> Result<WebResponse<DataQualitySnapshot>, WebError> {
    Ok(WebResponse::ok(state.data_quality.snapshot()))
}
