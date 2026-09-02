//! Route-local economic-health evidence endpoint.

use actix_web::{
    http::Method,
    web::{Data, Query},
};
use chrono::Utc;
use quant_pivot_models::{
    domain::{
        api::{EconomicHealthQuery, RouteEconomicHealthView},
        pagination::Paginated,
    },
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
        "/research/economic-health",
        Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
        get,
    )]
}

pub async fn get(
    state: Data<AppState>,
    query: Query<EconomicHealthQuery>,
) -> Result<WebResponse<Paginated<RouteEconomicHealthView>>, WebError> {
    let page = state
        .economic_feedback
        .route_health(query.into_inner(), Utc::now())
        .await?;
    Ok(WebResponse::ok(page))
}
