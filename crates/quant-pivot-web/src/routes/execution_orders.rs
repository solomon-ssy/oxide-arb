//! Execution order read API (venue submission records).

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::{
        api::{ExecutionOrderListQuery, ExecutionOrderView},
        pagination::Paginated,
    },
    enums::rbac::{Operation, ResourceType},
    types::ExecutionOrderId,
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
            "/quant/execution-orders",
            Rule::ResourceOp(ResourceType::ExecutionOrder, Operation::Read),
            list_execution_orders,
        ),
        spec(
            Method::GET,
            "/quant/execution-orders/{id}",
            Rule::ResourceOp(ResourceType::ExecutionOrder, Operation::Read),
            get_execution_order,
        ),
    ]
}

async fn list_execution_orders(
    state: Data<AppState>,
    query: Query<ExecutionOrderListQuery>,
) -> Result<WebResponse<Paginated<ExecutionOrderView>>, WebError> {
    let page = state
        .execution_read
        .list_execution_orders(query.into_inner())
        .await?;
    Ok(WebResponse::ok(page.map(ExecutionOrderView::from)))
}

async fn get_execution_order(
    state: Data<AppState>,
    id: Path<ExecutionOrderId>,
) -> Result<WebResponse<ExecutionOrderView>, WebError> {
    let info = state
        .execution_read
        .get_execution_order(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("execution order not found: {id}")))?;
    Ok(WebResponse::ok(ExecutionOrderView::from(info)))
}
