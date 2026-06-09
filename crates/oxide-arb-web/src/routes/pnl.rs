//! `PnL` dashboard read endpoints (`Pnl:Read`).
//!
//! Daily / weekly views deserialize the latest persisted settlement report
//! payload; the live snapshot reflects the in-memory risk-engine accounting.

use actix_web::{http::Method, web};
use oxide_arb_models::{
    domain::{DailyReport, LivePnlView, WeeklyReport},
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
            "/pnl/daily",
            Rule::ResourceOp(ResourceType::Pnl, Operation::Read),
            daily,
        ),
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
    ]
}

/// `GET /api/pnl/daily` — latest persisted daily settlement report.
pub async fn daily(state: web::Data<AppState>) -> Result<WebResponse<DailyReport>, WebError> {
    let report = state
        .reports
        .find_latest(ReportType::Daily)
        .await?
        .ok_or_else(|| WebError::NotFound("no daily report available yet".to_owned()))?;
    let parsed: DailyReport = serde_json::from_value(report.payload)
        .map_err(|error| WebError::Internal(format!("decode daily report: {error}")))?;
    Ok(WebResponse::ok(parsed))
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
