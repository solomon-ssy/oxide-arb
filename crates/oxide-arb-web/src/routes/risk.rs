//! Risk dashboard + control endpoints.
//!
//! Reads (`Risk:Read` / `Blacklist:Read`) surface the live risk-engine snapshot
//! (circuit breaker, exposure, daily loss), open positions, and the active
//! blacklist. Controls drive the live engine: circuit-breaker reset
//! (`Risk:Reset`, governed), blacklist add / remove (`Blacklist:Create` /
//! `Blacklist:Delete`, governed), each recorded on the operation log.

use actix_web::{http::Method, web};
use oxide_arb_models::{
    domain::{
        BlacklistCreateRequest, BlacklistInfo, BlacklistRemoveRequest, CircuitBreakerResetRequest,
        PositionView, RiskEngineStateView,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    types::{MarketId, Usd},
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

/// Risk dashboard + control routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/risk/circuit-breaker",
            Rule::ResourceOp(ResourceType::Risk, Operation::Read),
            circuit_breaker,
        ),
        spec(
            Method::GET,
            "/risk/positions",
            Rule::ResourceOp(ResourceType::Risk, Operation::Read),
            positions,
        ),
        spec(
            Method::GET,
            "/risk/exposure",
            Rule::ResourceOp(ResourceType::Risk, Operation::Read),
            exposure,
        ),
        spec(
            Method::GET,
            "/risk/daily-loss",
            Rule::ResourceOp(ResourceType::Risk, Operation::Read),
            daily_loss,
        ),
        spec(
            Method::POST,
            "/risk/circuit-breaker/reset",
            Rule::ActingRoleGoverned(ResourceType::Risk, Operation::Reset),
            reset_circuit_breaker,
        ),
        spec(
            Method::GET,
            "/risk/blacklist",
            Rule::ResourceOp(ResourceType::Blacklist, Operation::Read),
            list_blacklist,
        ),
        spec(
            Method::POST,
            "/risk/blacklist",
            Rule::ActingRoleGoverned(ResourceType::Blacklist, Operation::Create),
            add_blacklist,
        ),
        spec(
            Method::POST,
            "/risk/blacklist/{market_id}/remove",
            Rule::ActingRoleGoverned(ResourceType::Blacklist, Operation::Delete),
            remove_blacklist,
        ),
    ]
}

/// `GET /api/risk/circuit-breaker` — live risk-engine snapshot.
pub async fn circuit_breaker(
    state: web::Data<AppState>,
) -> Result<WebResponse<RiskEngineStateView>, WebError> {
    Ok(WebResponse::ok(RiskEngineStateView::from(
        state.control.risk_snapshot(),
    )))
}

/// `GET /api/risk/positions` — currently open positions.
pub async fn positions(
    state: web::Data<AppState>,
) -> Result<WebResponse<Vec<PositionView>>, WebError> {
    let open = state.positions.find_open().await?;
    Ok(WebResponse::ok(
        open.into_iter().map(PositionView::from).collect(),
    ))
}

/// `GET /api/risk/exposure` — live total exposure (positions + reservations).
pub async fn exposure(state: web::Data<AppState>) -> Result<WebResponse<Usd>, WebError> {
    Ok(WebResponse::ok(
        state.control.risk_snapshot().total_exposure,
    ))
}

/// `GET /api/risk/daily-loss` — accumulated daily loss magnitude.
pub async fn daily_loss(state: web::Data<AppState>) -> Result<WebResponse<Usd>, WebError> {
    Ok(WebResponse::ok(
        state.control.risk_snapshot().daily_loss_usd,
    ))
}

/// `POST /api/risk/circuit-breaker/reset` — force the breaker back to Closed.
pub async fn reset_circuit_breaker(
    state: web::Data<AppState>,
    _acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<CircuitBreakerResetRequest>,
) -> Result<WebResponse<()>, WebError> {
    let body = body.into_inner();
    op_ctx.set_action(OperationCategory::Risk, "risk.circuit_breaker.reset");
    op_ctx.set_detail(serde_json::json!({ "reason": body.reason }));
    state.control.reset_circuit_breaker(&body.reason).await?;
    Ok(WebResponse::ok(()))
}

/// `GET /api/risk/blacklist` — active blacklist entries.
pub async fn list_blacklist(
    state: web::Data<AppState>,
) -> Result<WebResponse<Vec<BlacklistInfo>>, WebError> {
    Ok(WebResponse::ok(state.control.blacklist()))
}

/// `POST /api/risk/blacklist` — add a market to the runtime blacklist.
pub async fn add_blacklist(
    state: web::Data<AppState>,
    _acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<BlacklistCreateRequest>,
) -> Result<WebResponse<()>, WebError> {
    let body = body.into_inner();
    op_ctx.set_action(OperationCategory::Risk, "risk.blacklist.add");
    op_ctx.set_resource(ResourceType::Blacklist, body.market_id.to_string());
    state
        .control
        .add_blacklist(body.market_id, body.reason)
        .await?;
    Ok(WebResponse::ok(()))
}

/// `POST /api/risk/blacklist/{market_id}/remove` — remove a market from the blacklist.
pub async fn remove_blacklist(
    state: web::Data<AppState>,
    market_id: web::Path<MarketId>,
    _acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<BlacklistRemoveRequest>,
) -> Result<WebResponse<()>, WebError> {
    let body = body.into_inner();
    op_ctx.set_action(OperationCategory::Risk, "risk.blacklist.remove");
    op_ctx.set_resource(ResourceType::Blacklist, market_id.to_string());
    op_ctx.set_detail(serde_json::json!({ "reason": body.reason }));
    state
        .control
        .remove_blacklist(&market_id, &body.reason)
        .await?;
    Ok(WebResponse::ok(()))
}
