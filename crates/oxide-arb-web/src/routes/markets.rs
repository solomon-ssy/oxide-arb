//! Markets dashboard read endpoints (`Market:Read`).
//!
//! Surfaces market metadata (paginated list + detail). The live order-book read
//! and the WS subscription controls are wired separately via the market-data
//! port (see [`crate::routes::markets::book`]).

use actix_web::{http::Method, web};
use oxide_arb_models::{
    domain::{MarketBookSideView, MarketBookView, MarketPageQuery, MarketView, Paginated},
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    types::MarketId,
};

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Markets dashboard routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/markets",
            Rule::ResourceOp(ResourceType::Market, Operation::Read),
            list,
        ),
        spec(
            Method::GET,
            "/markets/{market_id}/book",
            Rule::ResourceOp(ResourceType::Market, Operation::Read),
            book,
        ),
        spec(
            Method::POST,
            "/markets/{market_id}/subscribe",
            Rule::ResourceOp(ResourceType::Market, Operation::Update),
            subscribe,
        ),
        spec(
            Method::POST,
            "/markets/{market_id}/unsubscribe",
            Rule::ResourceOp(ResourceType::Market, Operation::Update),
            unsubscribe,
        ),
        spec(
            Method::GET,
            "/markets/{market_id}",
            Rule::ResourceOp(ResourceType::Market, Operation::Read),
            detail,
        ),
    ]
}

/// `GET /api/markets` — paginated, filtered market list (newest first).
pub async fn list(
    state: web::Data<AppState>,
    query: web::Query<MarketPageQuery>,
) -> Result<WebResponse<Paginated<MarketView>>, WebError> {
    let page = state.markets.page(query.into_inner().normalized()).await?;
    Ok(WebResponse::ok(Paginated {
        items: page.items.into_iter().map(MarketView::from).collect(),
        total: page.total,
        page: page.page,
        size: page.size,
        has_next: page.has_next,
    }))
}

/// `GET /api/markets/{market_id}` — single market detail.
pub async fn detail(
    state: web::Data<AppState>,
    market_id: web::Path<MarketId>,
) -> Result<WebResponse<MarketView>, WebError> {
    let market_id = market_id.into_inner();
    let market = state
        .markets
        .find_by_id(&market_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("market not found: {market_id}")))?;
    Ok(WebResponse::ok(MarketView::from((*market).clone())))
}

/// `GET /api/markets/{market_id}/book` — published YES/NO order books.
pub async fn book(
    state: web::Data<AppState>,
    market_id: web::Path<MarketId>,
) -> Result<WebResponse<MarketBookView>, WebError> {
    let market_id = market_id.into_inner();
    let market = state
        .markets
        .find_by_id(&market_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("market not found: {market_id}")))?;
    let (yes, no) = state
        .market_data
        .book(&market.yes_token_id, &market.no_token_id);
    Ok(WebResponse::ok(MarketBookView {
        market_id: market.market_id.clone(),
        yes: yes.map(|snapshot| {
            MarketBookSideView::from_snapshot(market.yes_token_id.clone(), &snapshot)
        }),
        no: no.map(|snapshot| {
            MarketBookSideView::from_snapshot(market.no_token_id.clone(), &snapshot)
        }),
    }))
}

/// `POST /api/markets/{market_id}/subscribe` — subscribe both tokens to the CLOB WS.
pub async fn subscribe(
    state: web::Data<AppState>,
    market_id: web::Path<MarketId>,
    op_ctx: OperationCtx,
) -> Result<WebResponse<()>, WebError> {
    let market_id = market_id.into_inner();
    op_ctx.set_action(OperationCategory::Other, "market.subscribe");
    op_ctx.set_resource(ResourceType::Market, market_id.to_string());
    let market = state
        .markets
        .find_by_id(&market_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("market not found: {market_id}")))?;
    state
        .market_data
        .subscribe(vec![
            market.yes_token_id.clone(),
            market.no_token_id.clone(),
        ])
        .await?;
    Ok(WebResponse::ok(()))
}

/// `POST /api/markets/{market_id}/unsubscribe` — unsubscribe both tokens.
pub async fn unsubscribe(
    state: web::Data<AppState>,
    market_id: web::Path<MarketId>,
    op_ctx: OperationCtx,
) -> Result<WebResponse<()>, WebError> {
    let market_id = market_id.into_inner();
    op_ctx.set_action(OperationCategory::Other, "market.unsubscribe");
    op_ctx.set_resource(ResourceType::Market, market_id.to_string());
    let market = state
        .markets
        .find_by_id(&market_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("market not found: {market_id}")))?;
    state
        .market_data
        .unsubscribe(vec![
            market.yes_token_id.clone(),
            market.no_token_id.clone(),
        ])
        .await?;
    Ok(WebResponse::ok(()))
}
