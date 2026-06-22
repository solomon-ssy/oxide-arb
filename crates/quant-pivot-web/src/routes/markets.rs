//! Markets dashboard read endpoints (`Market:Read`).
//!
//! Surfaces market metadata (paginated list + detail). The live order-book read
//! and the WS subscription controls are wired separately via the market-data
//! port (see [`crate::routes::markets::book`]).

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        MarketBookSideView, MarketBookSummaryView, MarketBookView, MarketDataPort, MarketInfo,
        MarketPageQuery, MarketView, Paginated,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    types::{MarketId, TokenId},
};
use std::collections::HashSet;

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

/// Project a persisted market row through the runtime overlay: live book
/// digest (published snapshots, lock-free) and CLOB WS subscription state.
/// `subscribed_union` must contain the union-live tokens for the whole batch.
fn project_market(
    market_data: &dyn MarketDataPort,
    subscribed_union: &HashSet<TokenId>,
    market: MarketInfo,
) -> MarketView {
    let (yes, no) = market_data.book(&market.yes_token_id, &market.no_token_id);
    let book = MarketBookSummaryView::from_snapshots(yes.as_deref(), no.as_deref());
    let subscribed = subscribed_union.contains(&market.yes_token_id)
        && subscribed_union.contains(&market.no_token_id);
    MarketView::project(market, subscribed, book)
}

/// `GET /api/markets` — paginated, filtered market list (newest first), each
/// row enriched with the live book digest and WS subscription state.
pub async fn list(
    state: web::Data<AppState>,
    query: web::Query<MarketPageQuery>,
) -> Result<WebResponse<Paginated<MarketView>>, WebError> {
    let page = state.markets.page(query.into_inner().normalized()).await?;
    let tokens: Vec<_> = page
        .items
        .iter()
        .flat_map(|m| [m.yes_token_id.clone(), m.no_token_id.clone()])
        .collect();
    let subscribed_union = state.market_data.subscribed_tokens(&tokens);
    Ok(WebResponse::ok(page.map(|market| {
        project_market(state.market_data.as_ref(), &subscribed_union, market)
    })))
}

/// `GET /api/markets/{market_id}` — single market detail with runtime overlay.
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
    let market = (*market).clone();
    let subscribed_union = state
        .market_data
        .subscribed_tokens(&[market.yes_token_id.clone(), market.no_token_id.clone()]);
    Ok(WebResponse::ok(project_market(
        state.market_data.as_ref(),
        &subscribed_union,
        market,
    )))
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
