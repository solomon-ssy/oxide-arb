//! Trades dashboard read endpoints (`Trade:Read`).
//!
//! Surfaces the persisted trade history (paginated list + detail) and the
//! risk-decision audit trail over a time window. All endpoints are read-only;
//! trades are written exclusively by the execution pipeline.

use actix_web::{http::Method, web};
use chrono::Duration;
use oxide_arb_models::{
    domain::{
        PageRequest, Paginated, RiskAuditEventView, TimeWindowQuery, TradePageQuery, TradeView,
    },
    enums::rbac::{Operation, ResourceType},
    types::TradeId,
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Default look-back for the risk-decision audit trail when `from` is omitted.
const DECISIONS_DEFAULT_WINDOW_DAYS: i64 = 7;
/// Maximum window span (days) accepted for a decisions query.
const DECISIONS_MAX_WINDOW_DAYS: i64 = 90;

/// Trades dashboard routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/trades",
            Rule::ResourceOp(ResourceType::Trade, Operation::Read),
            list,
        ),
        spec(
            Method::GET,
            "/trades/decisions",
            Rule::ResourceOp(ResourceType::Trade, Operation::Read),
            decisions,
        ),
        spec(
            Method::GET,
            "/trades/{trade_id}",
            Rule::ResourceOp(ResourceType::Trade, Operation::Read),
            detail,
        ),
    ]
}

/// `GET /api/trades` — paginated, filtered trade history (newest first).
pub async fn list(
    state: web::Data<AppState>,
    query: web::Query<TradePageQuery>,
) -> Result<WebResponse<Paginated<TradeView>>, WebError> {
    let page = state.trades.page(query.into_inner().normalized()).await?;
    Ok(WebResponse::ok(Paginated {
        items: page.items.into_iter().map(TradeView::from).collect(),
        total: page.total,
        page: page.page,
        size: page.size,
        has_next: page.has_next,
    }))
}

/// `GET /api/trades/{trade_id}` — single trade detail.
pub async fn detail(
    state: web::Data<AppState>,
    trade_id: web::Path<TradeId>,
) -> Result<WebResponse<TradeView>, WebError> {
    let trade_id = trade_id.into_inner();
    let trade = state
        .trades
        .find_by_id(&trade_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("trade not found: {trade_id}")))?;
    Ok(WebResponse::ok(TradeView::from(trade)))
}

/// `GET /api/trades/decisions?from=&to=&page=&size=` — paginated risk decision
/// audit events in a window (newest first).
pub async fn decisions(
    state: web::Data<AppState>,
    window: web::Query<TimeWindowQuery>,
    page: web::Query<PageRequest>,
) -> Result<WebResponse<Paginated<RiskAuditEventView>>, WebError> {
    let resolved = window.into_inner().resolve(
        Duration::days(DECISIONS_DEFAULT_WINDOW_DAYS),
        DECISIONS_MAX_WINDOW_DAYS,
    )?;
    let events = state
        .risk_audit
        .find_between_page(resolved, page.into_inner())
        .await?;
    Ok(WebResponse::ok(Paginated {
        items: events
            .items
            .into_iter()
            .map(RiskAuditEventView::from)
            .collect(),
        total: events.total,
        page: events.page,
        size: events.size,
        has_next: events.has_next,
    }))
}
