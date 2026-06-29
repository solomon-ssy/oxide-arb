//! Venue account read API (live + persisted snapshots).

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{AccountSnapshotView, LiveAccountView},
    enums::rbac::{Operation, ResourceType},
    types::AccountSnapshotId,
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
            "/quant/account/live",
            Rule::ResourceOp(ResourceType::AccountSnapshot, Operation::Read),
            live_account,
        ),
        spec(
            Method::GET,
            "/quant/account/snapshots/{id}",
            Rule::ResourceOp(ResourceType::AccountSnapshot, Operation::Read),
            get_snapshot,
        ),
    ]
}

async fn live_account(
    state: web::Data<AppState>,
) -> Result<WebResponse<LiveAccountView>, WebError> {
    let info = state.account_read.live_account().await?;
    Ok(WebResponse::ok(LiveAccountView::from_live(
        info.fetched_at,
        info.budget_cap_usd,
        info.snapshot,
    )))
}

async fn get_snapshot(
    state: web::Data<AppState>,
    id: web::Path<AccountSnapshotId>,
) -> Result<WebResponse<AccountSnapshotView>, WebError> {
    let info = state
        .account_read
        .find_snapshot_by_id(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("account snapshot not found: {id}")))?;
    Ok(WebResponse::ok(AccountSnapshotView::from(info)))
}
