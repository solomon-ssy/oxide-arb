//! System lot position ledger read API with per-intent lots.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::{
        api::{OrderIntentView, PositionDetailView, PositionListQuery, PositionView},
        pagination::Paginated,
    },
    enums::rbac::{Operation, ResourceType},
    types::PositionId,
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::{
        quant_intents::exit_monitor_observation,
        registry::{RouteSpec, spec},
    },
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
    state: Data<AppState>,
    query: Query<PositionListQuery>,
) -> Result<WebResponse<Paginated<PositionView>>, WebError> {
    let page = state
        .execution_read
        .list_positions(query.into_inner())
        .await?;
    Ok(WebResponse::ok(page.map(PositionView::from)))
}

async fn get_position(
    state: Data<AppState>,
    id: Path<PositionId>,
) -> Result<WebResponse<PositionDetailView>, WebError> {
    let summary = state
        .execution_read
        .get_position(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("position not found: {id}")))?;
    let intent = state
        .order_intents
        .find(&summary.position.order_intent_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!(
                "order intent not found: {}",
                summary.position.order_intent_id
            ))
        })?;
    let intent_view = OrderIntentView::from(intent);
    let exit_monitor_observation =
        exit_monitor_observation(&state, &intent_view, &summary.position).await?;
    Ok(WebResponse::ok(PositionDetailView {
        position: PositionView::from(summary),
        exit_monitor_observation,
    }))
}
