//! Venue account read API (live + persisted snapshots).

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::{
        api::{AccountSnapshotView, EquitySnapshotView, LiveAccountView},
        pagination::Paginated,
        quant::EquitySnapshotQuery,
    },
    enums::rbac::{Operation, ResourceType},
    types::{AccountSnapshotId, EquitySnapshotId},
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
        spec(
            Method::GET,
            "/quant/account/equity-snapshots/latest",
            Rule::ResourceOp(ResourceType::EquitySnapshot, Operation::Read),
            latest_equity_snapshot,
        ),
        spec(
            Method::GET,
            "/quant/account/equity-snapshots/{id}",
            Rule::ResourceOp(ResourceType::EquitySnapshot, Operation::Read),
            get_equity_snapshot,
        ),
        spec(
            Method::GET,
            "/quant/account/equity-snapshots",
            Rule::ResourceOp(ResourceType::EquitySnapshot, Operation::Read),
            list_equity_snapshots,
        ),
    ]
}

async fn live_account(state: Data<AppState>) -> Result<WebResponse<LiveAccountView>, WebError> {
    let info = state.account_read.live_account().await?;
    Ok(WebResponse::ok(LiveAccountView::from_live(
        info.fetched_at,
        info.budget_cap_usd,
        info.snapshot,
    )))
}

async fn get_snapshot(
    state: Data<AppState>,
    id: Path<AccountSnapshotId>,
) -> Result<WebResponse<AccountSnapshotView>, WebError> {
    let info = state
        .account_read
        .find_snapshot_by_id(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("account snapshot not found: {id}")))?;
    Ok(WebResponse::ok(AccountSnapshotView::from(info)))
}

async fn latest_equity_snapshot(
    state: Data<AppState>,
) -> Result<WebResponse<EquitySnapshotView>, WebError> {
    let info = state
        .account_read
        .latest_equity_snapshot()
        .await?
        .ok_or_else(|| WebError::NotFound("equity snapshot not found".to_owned()))?;
    Ok(WebResponse::ok(EquitySnapshotView::from(info)))
}

async fn get_equity_snapshot(
    state: Data<AppState>,
    id: Path<EquitySnapshotId>,
) -> Result<WebResponse<EquitySnapshotView>, WebError> {
    let info = state
        .account_read
        .find_equity_snapshot_by_id(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("equity snapshot not found: {id}")))?;
    Ok(WebResponse::ok(EquitySnapshotView::from(info)))
}

async fn list_equity_snapshots(
    state: Data<AppState>,
    query: Query<EquitySnapshotQuery>,
) -> Result<WebResponse<Paginated<EquitySnapshotView>>, WebError> {
    let page = state
        .account_read
        .equity_snapshots(query.into_inner())
        .await?
        .map(EquitySnapshotView::from);
    Ok(WebResponse::ok(page))
}
