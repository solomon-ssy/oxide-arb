//! Settlement-redeem read API; only the worker writes this ledger.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::{
        api::{
            SettlementRedeemDetailView, SettlementRedeemListQuery, SettlementRedeemLotView,
            SettlementRedeemSummary, SettlementRedeemView,
        },
        pagination::Paginated,
    },
    enums::rbac::{Operation, ResourceType},
    types::SettlementRedeemId,
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
            "/quant/settlement-redeems",
            Rule::ResourceOp(ResourceType::SettlementRedeem, Operation::Read),
            list,
        ),
        spec(
            Method::GET,
            "/quant/settlement-redeems/{id}",
            Rule::ResourceOp(ResourceType::SettlementRedeem, Operation::Read),
            get,
        ),
    ]
}

async fn list(
    state: Data<AppState>,
    query: Query<SettlementRedeemListQuery>,
) -> Result<WebResponse<Paginated<SettlementRedeemView>>, WebError> {
    let page = state
        .execution_read
        .list_settlement_redeems(query.into_inner())
        .await?;
    Ok(WebResponse::ok(page.map(SettlementRedeemView::from)))
}

async fn get(
    state: Data<AppState>,
    id: Path<SettlementRedeemId>,
) -> Result<WebResponse<SettlementRedeemDetailView>, WebError> {
    let detail = state
        .execution_read
        .get_settlement_redeem(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("settlement redeem not found: {id}")))?;
    let lot_count = i64::try_from(detail.lots.len()).unwrap_or(i64::MAX);
    let lots = detail
        .lots
        .into_iter()
        .map(SettlementRedeemLotView::from)
        .collect();
    Ok(WebResponse::ok(SettlementRedeemDetailView {
        redeem: SettlementRedeemView::from(SettlementRedeemSummary {
            redeem: detail.redeem,
            lot_count,
        }),
        lots,
    }))
}
