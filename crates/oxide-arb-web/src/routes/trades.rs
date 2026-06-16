//! Trades dashboard read endpoints (`Trade:Read`).
//!
//! Surfaces the persisted trade history (paginated list + detail) and the
//! risk-decision audit trail over a time window. All endpoints are read-only;
//! trades are written exclusively by the execution pipeline.

use actix_web::{http::Method, web};
use chrono::Duration;
use oxide_arb_models::{
    domain::{
        PageRequest, Paginated, ReconcileTradeRequest, RiskAuditEventView, TimeWindowQuery,
        TradePageQuery, TradeView,
    },
    enums::{
        common::TradeReconcileResolution,
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    types::TradeId,
};

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, ValidatedJson},
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
            "/trades/reconciliation",
            Rule::ResourceOp(ResourceType::Trade, Operation::Read),
            reconciliation,
        ),
        spec(
            Method::POST,
            "/trades/{trade_id}/reconcile",
            Rule::ActingRoleGoverned(ResourceType::Trade, Operation::Update),
            reconcile_trade,
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

/// `GET /api/trades/reconciliation` — unresolved unknown venue outcomes.
pub async fn reconciliation(
    state: web::Data<AppState>,
    page: web::Query<PageRequest>,
) -> Result<WebResponse<Paginated<TradeView>>, WebError> {
    let window = page.into_inner().normalized();
    let items = state.trades.find_needs_reconcile(window.limit()).await?;
    let total = u64::try_from(items.len()).unwrap_or(u64::MAX);
    Ok(WebResponse::ok(Paginated::from_request(
        items.into_iter().map(TradeView::from).collect(),
        total,
        &window,
    )))
}

/// `POST /api/trades/{trade_id}/reconcile` — manually close an ambiguous trade.
pub async fn reconcile_trade(
    state: web::Data<AppState>,
    trade_id: web::Path<TradeId>,
    acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<ReconcileTradeRequest>,
) -> Result<WebResponse<TradeView>, WebError> {
    let trade_id = trade_id.into_inner();
    let body = body.into_inner();
    if body.resolution != TradeReconcileResolution::Unresolvable {
        return Err(WebError::BadRequest(
            "manual reconciliation currently only accepts unresolvable; filled/miss must be proven by external evidence"
                .into(),
        ));
    }
    op_ctx.set_action(OperationCategory::Risk, "trade.reconcile");
    op_ctx.set_detail(serde_json::json!({
        "trade_id": trade_id,
        "resolution": body.resolution.as_str(),
        "note": body.note.clone(),
        "acting_role": acting_role.0,
    }));
    let updated = state
        .control
        .close_unresolvable_trade(&trade_id, &body.note, &acting_role.0)
        .await?;
    if !updated {
        return Err(WebError::Conflict(format!(
            "trade {trade_id} is not pending reconciliation"
        )));
    }
    let trade = state
        .trades
        .find_by_id(&trade_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("trade not found: {trade_id}")))?;
    Ok(WebResponse::ok(TradeView::from(trade)))
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
