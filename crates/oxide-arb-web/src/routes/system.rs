//! System status + control endpoints.
//!
//! Reads (`System:Read`) project the live aggregate status / health views.
//! Controls are money-critical: `halt` / `resume` engage the risk halt + the
//! execution kill switch, and the execution-mode hot-swap is **governed**
//! (`ActingRoleGoverned(System, SwitchMode)`) — entering `Live` is the highest
//! risk operator action, so it requires the strict acting-role authorization and
//! a mandatory reason, and is recorded on the operation log.

use actix_web::{http::Method, web};
use oxide_arb_models::{
    domain::{
        CoreEvent, HaltRequest, HealthReport, ModeTransitionReport, ResumeRequest,
        SwitchModeRequest, SystemStatus,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
};

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, AuthedActor, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// System status + control routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/system/status",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            status,
        ),
        spec(
            Method::GET,
            "/system/health",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            health,
        ),
        spec(
            Method::POST,
            "/system/halt",
            Rule::ResourceOp(ResourceType::System, Operation::Halt),
            halt,
        ),
        spec(
            Method::POST,
            "/system/resume",
            Rule::ResourceOp(ResourceType::System, Operation::Resume),
            resume,
        ),
        spec(
            Method::POST,
            "/system/mode",
            Rule::ActingRoleGoverned(ResourceType::System, Operation::SwitchMode),
            switch_mode,
        ),
    ]
}

/// `GET /api/system/status` — aggregate live system status.
pub async fn status(state: web::Data<AppState>) -> Result<WebResponse<SystemStatus>, WebError> {
    Ok(WebResponse::ok(state.control.system_status().await))
}

/// `GET /api/system/health` — subsystem health report.
pub async fn health(state: web::Data<AppState>) -> Result<WebResponse<HealthReport>, WebError> {
    Ok(WebResponse::ok(state.control.health().await))
}

/// `POST /api/system/halt` — halt trading (risk halt + execution kill switch).
pub async fn halt(
    state: web::Data<AppState>,
    op_ctx: OperationCtx,
    body: ValidatedJson<HaltRequest>,
) -> Result<WebResponse<()>, WebError> {
    let body = body.into_inner();
    op_ctx.set_action(OperationCategory::System, "system.halt");
    op_ctx.set_detail(serde_json::json!({ "reason": body.reason }));
    state.control.halt(body.reason).await;
    publish_status(&state).await;
    Ok(WebResponse::ok(()))
}

/// `POST /api/system/resume` — resume trading after operator acknowledgement.
pub async fn resume(
    state: web::Data<AppState>,
    op_ctx: OperationCtx,
    body: ValidatedJson<ResumeRequest>,
) -> Result<WebResponse<()>, WebError> {
    let body = body.into_inner();
    op_ctx.set_action(OperationCategory::System, "system.resume");
    state.control.resume(&body.operator_ack).await?;
    publish_status(&state).await;
    Ok(WebResponse::ok(()))
}

/// `POST /api/system/mode` — governed runtime execution-mode hot-swap.
///
/// `ActingRoleGoverned`, so authz has already resolved an [`ActingRole`]; the
/// operator's user id is used as the acknowledgement recorded on the risk audit.
pub async fn switch_mode(
    state: web::Data<AppState>,
    actor: AuthedActor,
    _acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<SwitchModeRequest>,
) -> Result<WebResponse<ModeTransitionReport>, WebError> {
    let body = body.into_inner();
    op_ctx.set_action(OperationCategory::System, "system.switch_mode");
    op_ctx.set_detail(serde_json::json!({
        "target_mode": body.mode.as_str(),
        "reason": body.reason,
    }));
    let report = state
        .control
        .switch_execution_mode(body.mode, &actor.claims.sub)
        .await?;
    publish_status(&state).await;
    Ok(WebResponse::ok(report))
}

/// Publish a fresh `SystemStatusChanged` event after a control action so live
/// WebSocket clients observe the new state immediately.
async fn publish_status(state: &AppState) {
    let status = state.control.system_status().await;
    state.events.publish(CoreEvent::SystemStatusChanged(status));
}
