//! Opportunities dashboard read endpoints (`Opportunity:Read`).
//!
//! Backed by the `ClickHouse` evidence timeseries: detections (recent / history),
//! per-opportunity audit trail (detail), and the audit funnel (stats). Every
//! list endpoint is paginated (`?page=&size=`) and time-windowed (`?from=&to=`,
//! clamped to [`MAX_WINDOW_DAYS`]); the repository enforces a stable ordering.

use actix_web::{http::Method, web};
use chrono::{Duration, Utc};
use oxide_arb_models::{
    clickhouse::{OpportunityAuditRow, OpportunityDetectionRow},
    domain::{MarketFilter, PageRequest, Paginated, TimeWindow, TimeWindowQuery},
    enums::rbac::{Operation, ResourceType},
    types::OpportunityId,
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Default look-back for the "recent" detections view.
const RECENT_LOOKBACK_HOURS: i64 = 24;
/// Default look-back for the history / stats views when `from` is omitted.
const DEFAULT_WINDOW_DAYS: i64 = 7;
/// Maximum window span (days) accepted for a history/stats query.
const MAX_WINDOW_DAYS: i64 = 90;

/// Opportunities dashboard routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/opportunities/recent",
            Rule::ResourceOp(ResourceType::Opportunity, Operation::Read),
            recent,
        ),
        spec(
            Method::GET,
            "/opportunities/history",
            Rule::ResourceOp(ResourceType::Opportunity, Operation::Read),
            history,
        ),
        spec(
            Method::GET,
            "/opportunities/stats",
            Rule::ResourceOp(ResourceType::Opportunity, Operation::Read),
            stats,
        ),
        spec(
            Method::GET,
            "/opportunities/{opportunity_id}",
            Rule::ResourceOp(ResourceType::Opportunity, Operation::Read),
            detail,
        ),
    ]
}

/// `GET /api/opportunities/recent?page=&size=` — detections in the last 24h.
pub async fn recent(
    state: web::Data<AppState>,
    page: web::Query<PageRequest>,
) -> Result<WebResponse<Paginated<OpportunityDetectionRow>>, WebError> {
    let window = TimeWindow::new(
        Utc::now() - Duration::hours(RECENT_LOOKBACK_HOURS),
        Utc::now(),
    );
    let result = state
        .evidence
        .detections_page(MarketFilter::default(), window, page.into_inner())
        .await?;
    Ok(WebResponse::ok(result))
}

/// `GET /api/opportunities/history?from=&to=&market_id=&page=&size=` — detections in a window.
pub async fn history(
    state: web::Data<AppState>,
    window: web::Query<TimeWindowQuery>,
    page: web::Query<PageRequest>,
) -> Result<WebResponse<Paginated<OpportunityDetectionRow>>, WebError> {
    let window = window.into_inner();
    let resolved = window.resolve(Duration::days(DEFAULT_WINDOW_DAYS), MAX_WINDOW_DAYS)?;
    let result = state
        .evidence
        .detections_page(window.market_filter(), resolved, page.into_inner())
        .await?;
    Ok(WebResponse::ok(result))
}

/// `GET /api/opportunities/stats?from=&to=&market_id=&page=&size=` — audit funnel rows.
pub async fn stats(
    state: web::Data<AppState>,
    window: web::Query<TimeWindowQuery>,
    page: web::Query<PageRequest>,
) -> Result<WebResponse<Paginated<OpportunityAuditRow>>, WebError> {
    let window = window.into_inner();
    let resolved = window.resolve(Duration::days(DEFAULT_WINDOW_DAYS), MAX_WINDOW_DAYS)?;
    let result = state
        .evidence
        .audit_funnel_page(window.market_filter(), resolved, page.into_inner())
        .await?;
    Ok(WebResponse::ok(result))
}

/// `GET /api/opportunities/{opportunity_id}` — audit trail for one opportunity.
pub async fn detail(
    state: web::Data<AppState>,
    opportunity_id: web::Path<OpportunityId>,
) -> Result<WebResponse<Vec<OpportunityAuditRow>>, WebError> {
    let result = state
        .evidence
        .audits(std::slice::from_ref(&opportunity_id.into_inner()))
        .await?;
    let rows = result.into_rows();
    if rows.is_empty() {
        return Err(WebError::NotFound("opportunity not found".to_owned()));
    }
    Ok(WebResponse::ok(rows))
}
