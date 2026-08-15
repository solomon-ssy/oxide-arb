//! Structural Alpha monitor endpoints.
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/quant/structural/negrisk-events` | `quant_report:read` | Live per-event leg-sum drift |
//! | GET | `/quant/structural/execution-history/coverage` | `quant_report:read` | Finalized execution-history coverage/lag |
//! | GET | `/quant/structural/participant-concentration` | `quant_report:read` | Top concentration markets |
//! | GET | `/quant/structural/participant-concentration/{market_id}` | `quant_report:read` | Market participant detail |

use actix_web::{
    http::Method,
    web::{Data, Path},
};
use quant_pivot_models::{
    domain::api::{
        ExecutionHistoryCoverageView, NegRiskEventDriftView, ParticipantConcentrationDetailView,
        ParticipantConcentrationSummaryView,
    },
    enums::rbac::{Operation, ResourceType},
    types::MarketId,
};

use crate::{
    auth::casbin::Rule,
    error::WebError,
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Structural-monitor routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/quant/structural/negrisk-events",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            negrisk_events,
        ),
        spec(
            Method::GET,
            "/quant/structural/execution-history/coverage",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            execution_history_coverage,
        ),
        spec(
            Method::GET,
            "/quant/structural/participant-concentration",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            participant_concentration,
        ),
        spec(
            Method::GET,
            "/quant/structural/participant-concentration/{market_id}",
            Rule::ResourceOp(ResourceType::QuantReport, Operation::Read),
            participant_concentration_market,
        ),
    ]
}

/// `GET /api/quant/structural/negrisk-events` — live neg-risk leg-sum drift,
/// most-mispriced first.
pub async fn negrisk_events(
    state: Data<AppState>,
) -> Result<WebResponse<Vec<NegRiskEventDriftView>>, WebError> {
    let events = state.structural_monitor.negrisk_events().await?;
    Ok(WebResponse::ok(events))
}

pub async fn execution_history_coverage(
    state: Data<AppState>,
) -> Result<WebResponse<ExecutionHistoryCoverageView>, WebError> {
    let coverage = state
        .structural_monitor
        .execution_history_coverage()
        .await?;
    Ok(WebResponse::ok(coverage))
}

pub async fn participant_concentration(
    state: Data<AppState>,
) -> Result<WebResponse<ParticipantConcentrationSummaryView>, WebError> {
    let summary = state.structural_monitor.participant_concentration().await?;
    Ok(WebResponse::ok(summary))
}

pub async fn participant_concentration_market(
    state: Data<AppState>,
    market_id: Path<MarketId>,
) -> Result<WebResponse<Option<ParticipantConcentrationDetailView>>, WebError> {
    let detail = state
        .structural_monitor
        .participant_concentration_market(&market_id.into_inner())
        .await?;
    Ok(WebResponse::ok(detail))
}
