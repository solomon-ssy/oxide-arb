//! Analytics dashboard read endpoints (`Analytics:Read`).
//!
//! Daily / weekly reuse the persisted settlement reports; edge-distribution and
//! market-performance are computed on demand from the trade history over a
//! bounded window.

use actix_web::{http::Method, web};
use chrono::Duration;
use oxide_arb_models::{
    domain::{
        DailyReport, EdgeBucket, MarketPerformanceRow, PageRequest, Paginated, TimeWindowQuery,
        WeeklyReport,
    },
    enums::{
        common::ReportType,
        rbac::{Operation, ResourceType},
    },
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Maximum analytics window span (days).
const MAX_WINDOW_DAYS: i64 = 90;
/// Default analytics window when unspecified.
const DEFAULT_WINDOW_DAYS: i64 = 7;

/// Analytics dashboard routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/analytics/daily",
            Rule::ResourceOp(ResourceType::Analytics, Operation::Read),
            daily,
        ),
        spec(
            Method::GET,
            "/analytics/weekly",
            Rule::ResourceOp(ResourceType::Analytics, Operation::Read),
            weekly,
        ),
        spec(
            Method::GET,
            "/analytics/edge-distribution",
            Rule::ResourceOp(ResourceType::Analytics, Operation::Read),
            edge_distribution,
        ),
        spec(
            Method::GET,
            "/analytics/market-performance",
            Rule::ResourceOp(ResourceType::Analytics, Operation::Read),
            market_performance,
        ),
    ]
}

/// `GET /api/analytics/daily` — latest daily settlement report.
pub async fn daily(state: web::Data<AppState>) -> Result<WebResponse<DailyReport>, WebError> {
    let report = state
        .reports
        .find_latest(ReportType::Daily)
        .await?
        .ok_or_else(|| WebError::NotFound("no daily report available yet".to_owned()))?;
    let parsed = serde_json::from_value(report.payload)
        .map_err(|error| WebError::Internal(format!("decode daily report: {error}")))?;
    Ok(WebResponse::ok(parsed))
}

/// `GET /api/analytics/weekly` — latest weekly settlement report.
pub async fn weekly(state: web::Data<AppState>) -> Result<WebResponse<WeeklyReport>, WebError> {
    let report = state
        .reports
        .find_latest(ReportType::Weekly)
        .await?
        .ok_or_else(|| WebError::NotFound("no weekly report available yet".to_owned()))?;
    let parsed = serde_json::from_value(report.payload)
        .map_err(|error| WebError::Internal(format!("decode weekly report: {error}")))?;
    Ok(WebResponse::ok(parsed))
}

/// `GET /api/analytics/edge-distribution?from=&to=` — detected-edge histogram
/// over the trade history in the window (aggregated SQL-side).
pub async fn edge_distribution(
    state: web::Data<AppState>,
    window: web::Query<TimeWindowQuery>,
) -> Result<WebResponse<Vec<EdgeBucket>>, WebError> {
    let resolved = window
        .into_inner()
        .resolve(Duration::days(DEFAULT_WINDOW_DAYS), MAX_WINDOW_DAYS)?;
    let distribution = state.trades.edge_histogram(resolved).await?;
    Ok(WebResponse::ok(distribution))
}

/// `GET /api/analytics/market-performance?from=&to=&page=&size=` — per-market
/// aggregates, computed and paginated SQL-side (ordered by net profit desc).
pub async fn market_performance(
    state: web::Data<AppState>,
    window: web::Query<TimeWindowQuery>,
    page: web::Query<PageRequest>,
) -> Result<WebResponse<Paginated<MarketPerformanceRow>>, WebError> {
    let resolved = window
        .into_inner()
        .resolve(Duration::days(DEFAULT_WINDOW_DAYS), MAX_WINDOW_DAYS)?;
    let rows = state
        .trades
        .market_performance(resolved, page.into_inner())
        .await?;
    Ok(WebResponse::ok(rows))
}
