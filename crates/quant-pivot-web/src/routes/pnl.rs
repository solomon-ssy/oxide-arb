//! `PnL` dashboard read endpoints (`Pnl:Read`).
//!
//! The weekly view deserializes the latest persisted settlement report; the
//! daily series projects recent daily reports for charting; the live snapshot
//! reflects the in-memory risk-engine accounting. Full daily reports live under
//! [`crate::routes::analytics`].

use actix_web::{http::Method, web};
use oxide_arb_models::{
    domain::{DailyPnlSeries, DailyPnlSeriesQuery, DailyReport, LivePnlView, WeeklyReport},
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

/// `PnL` dashboard routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/pnl/weekly",
            Rule::ResourceOp(ResourceType::Pnl, Operation::Read),
            weekly,
        ),
        spec(
            Method::GET,
            "/pnl/live",
            Rule::ResourceOp(ResourceType::Pnl, Operation::Read),
            live,
        ),
        spec(
            Method::GET,
            "/pnl/daily-series",
            Rule::ResourceOp(ResourceType::Pnl, Operation::Read),
            daily_series,
        ),
    ]
}

/// `GET /api/pnl/weekly` — latest persisted weekly settlement report.
pub async fn weekly(state: web::Data<AppState>) -> Result<WebResponse<WeeklyReport>, WebError> {
    let report = state
        .reports
        .find_latest(ReportType::Weekly)
        .await?
        .ok_or_else(|| WebError::NotFound("no weekly report available yet".to_owned()))?;
    let parsed: WeeklyReport = serde_json::from_value(report.payload)
        .map_err(|error| WebError::Internal(format!("decode weekly report: {error}")))?;
    Ok(WebResponse::ok(parsed))
}

/// `GET /api/pnl/live` — live in-memory `PnL` snapshot.
pub async fn live(state: web::Data<AppState>) -> Result<WebResponse<LivePnlView>, WebError> {
    Ok(WebResponse::ok(LivePnlView::from(
        &state.control.risk_snapshot(),
    )))
}

/// `GET /api/pnl/daily-series?days=7` — per-day settled `PnL` history,
/// ascending by date with a running window total.
///
/// Returns `200` with an empty `points` array when no daily report exists yet
/// (the dashboard renders an empty chart state instead of handling a 404).
pub async fn daily_series(
    state: web::Data<AppState>,
    query: web::Query<DailyPnlSeriesQuery>,
) -> Result<WebResponse<DailyPnlSeries>, WebError> {
    let days = query.resolve_days()?;
    let reports = state
        .reports
        .find_by_type(ReportType::Daily, u64::from(days))
        .await?;
    let mut parsed = Vec::with_capacity(reports.len());
    for report in reports {
        let daily: DailyReport = serde_json::from_value(report.payload)
            .map_err(|error| WebError::Internal(format!("decode daily report: {error}")))?;
        parsed.push(daily);
    }
    Ok(WebResponse::ok(DailyPnlSeries::from_daily_reports(parsed)))
}
