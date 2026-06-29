//! System lot position ledger read API (per-intent R3 lots).

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{Paginated, PositionListQuery, PositionView},
    enums::rbac::{Operation, ResourceType},
    types::PositionId,
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/quant/positions",
            Rule::ResourceOp(ResourceType::Position, Operation::Read),
            list_positions,
        ),
        spec(
            Method::GET,
            "/quant/positions/{id}",
            Rule::ResourceOp(ResourceType::Position, Operation::Read),
            get_position,
        ),
    ]
}

async fn list_positions(
    state: web::Data<AppState>,
    query: web::Query<PositionListQuery>,
) -> Result<WebResponse<Paginated<PositionView>>, WebError> {
    let page = state
        .execution_read
        .list_positions(query.into_inner().normalized())
        .await?;
    Ok(WebResponse::ok(page.map(PositionView::from)))
}

async fn get_position(
    state: web::Data<AppState>,
    id: web::Path<PositionId>,
) -> Result<WebResponse<PositionView>, WebError> {
    let info = state
        .execution_read
        .get_position(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("position not found: {id}")))?;
    Ok(WebResponse::ok(PositionView::from(info)))
}
