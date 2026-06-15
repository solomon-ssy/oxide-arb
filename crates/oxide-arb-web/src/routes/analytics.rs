//! Analytics dashboard read endpoints (`Analytics:Read`).
//!
//! All windowed routes share [`AnalyticsQuery`] → [`AnalyticsScope`]:
//! settlement charts read persisted daily reports; execution aggregates read
//! the `trade` table over the same half-open UTC execution window.

use actix_web::{http::Method, web};
use chrono::Duration;
use oxide_arb_models::{
    domain::{
        AnalyticsDailySeries, AnalyticsQuery, EdgeBucket, MarketPerformanceRow, PageRequest,
        Paginated, WeeklyReport,
    },
    enums::common::ReportType,
    enums::rbac::{Operation, ResourceType},
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

/// `GET /api/analytics/daily` — settlement-basis daily `PnL` series (ascending).
pub async fn daily(
    state: web::Data<AppState>,
    query: web::Query<AnalyticsQuery>,
) -> Result<WebResponse<AnalyticsDailySeries>, WebError> {
    let scope = query
        .into_inner()
        .resolve(Duration::days(DEFAULT_WINDOW_DAYS), MAX_WINDOW_DAYS)?;
    let reports = state
        .reports
        .find_daily_between(scope.settlement_from, scope.settlement_to)
        .await?;
    let mut parsed = Vec::with_capacity(reports.len());
    for report in reports {
        let daily = serde_json::from_value(report.payload)
            .map_err(|error| WebError::Internal(format!("decode daily report: {error}")))?;
        parsed.push(daily);
    }
    Ok(WebResponse::ok(AnalyticsDailySeries::from_daily_reports(
        parsed,
    )))
}

/// `GET /api/analytics/weekly` — latest weekly settlement report.
pub async fn weekly(
    state: web::Data<AppState>,
) -> Result<WebResponse<Option<WeeklyReport>>, WebError> {
    let Some(report) = state.reports.find_latest(ReportType::Weekly).await? else {
        return Ok(WebResponse::ok(None));
    };
    let parsed = serde_json::from_value(report.payload)
        .map_err(|error| WebError::Internal(format!("decode weekly report: {error}")))?;
    Ok(WebResponse::ok(Some(parsed)))
}

/// `GET /api/analytics/edge-distribution` — execution-basis edge histogram.
pub async fn edge_distribution(
    state: web::Data<AppState>,
    query: web::Query<AnalyticsQuery>,
) -> Result<WebResponse<Vec<EdgeBucket>>, WebError> {
    let scope = query
        .into_inner()
        .resolve(Duration::days(DEFAULT_WINDOW_DAYS), MAX_WINDOW_DAYS)?;
    let distribution = state.trades.edge_histogram(scope.trade_filter()).await?;
    Ok(WebResponse::ok(distribution))
}

/// `GET /api/analytics/market-performance` — execution-basis per-market rollup.
pub async fn market_performance(
    state: web::Data<AppState>,
    query: web::Query<AnalyticsQuery>,
    page: web::Query<PageRequest>,
) -> Result<WebResponse<Paginated<MarketPerformanceRow>>, WebError> {
    let scope = query
        .into_inner()
        .resolve(Duration::days(DEFAULT_WINDOW_DAYS), MAX_WINDOW_DAYS)?;
    let rows = state
        .trades
        .market_performance(scope.trade_filter(), page.into_inner())
        .await?;
    Ok(WebResponse::ok(rows))
}
