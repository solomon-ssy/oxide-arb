//! System status + quant runtime mode endpoints (Phase 0).

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        ActionEligibilityDecision, ActionEligibilityView, ActivateBootstrapRequest, BootstrapView,
        CapabilityView, ExecutionRecoveryView, HealthReport, KillSwitchView,
        QuantModeTransitionReport, QuantModeView, SetKillSwitchCommand, SetKillSwitchRequest,
        SwitchQuantModeRequest, SystemStatusView,
    },
    enums::{
        execution::KillSwitchState,
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    hashing::CanonicalDigest,
    types::ContentHash,
};
use serde::Serialize;

use crate::{
    audit::OperationCtx,
    auth::casbin::{Rule, checker::SUPER_ADMIN_ROLE},
    error::WebError,
    extractors::{ActingRole, AuthedActor, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

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
            "/system/action-eligibility",
            Rule::AuthenticatedOnly,
            action_eligibility,
        ),
        spec(
            Method::POST,
            "/system/bootstrap/activate",
            Rule::ActingRoleGoverned(ResourceType::System, Operation::BootstrapActivate),
            activate_bootstrap,
        ),
        spec(
            Method::GET,
            "/system/health",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            health,
        ),
        spec(
            Method::GET,
            "/system/quant-mode",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            quant_mode,
        ),
        spec(
            Method::POST,
            "/system/quant-mode",
            Rule::ActingRoleGoverned(ResourceType::System, Operation::SwitchMode),
            switch_quant_mode,
        ),
        spec(
            Method::GET,
            "/system/kill-switch",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            kill_switch_status,
        ),
        spec(
            Method::POST,
            "/system/kill-switch",
            // The required permission depends on the transition (halt / resume /
            // emergency), computed in the handler once the target state is known.
            Rule::ActingRoleDeferred(ResourceType::System),
            set_kill_switch,
        ),
        spec(
            Method::GET,
            "/system/execution-recovery",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            execution_recovery,
        ),
    ]
}

pub async fn status(state: web::Data<AppState>) -> Result<WebResponse<SystemStatusView>, WebError> {
    let runtime = state.control.system_status();
    let capabilities = state.bootstrap.capabilities(&runtime).await?;
    Ok(WebResponse::ok(SystemStatusView {
        runtime,
        bootstrap: state.bootstrap.view(),
        capabilities,
    }))
}

pub async fn action_eligibility(
    state: web::Data<AppState>,
    actor: AuthedActor,
) -> Result<WebResponse<ActionEligibilityView>, WebError> {
    let runtime = state.control.system_status();
    let capabilities = state.bootstrap.capabilities(&runtime).await?;
    let subject = &actor.claims.sub;
    let report_permission = state
        .casbin
        .enforce(
            subject,
            ResourceType::QuantReport.as_str(),
            Operation::Enqueue.as_str(),
        )
        .await?;
    let entry_permission = state
        .casbin
        .enforce(
            subject,
            ResourceType::OrderIntent.as_str(),
            Operation::Create.as_str(),
        )
        .await?;
    let order_permission = state
        .casbin
        .enforce(
            subject,
            ResourceType::ExecutionOrder.as_str(),
            Operation::Create.as_str(),
        )
        .await?;
    Ok(WebResponse::ok(ActionEligibilityView {
        capability_revision: capabilities.revision,
        report_generation: action_decision(
            report_permission,
            capabilities.report_generation_eligible,
        ),
        entry_admission: action_decision(entry_permission, capabilities.entry_admission_eligible),
        order_submission: action_decision(order_permission, capabilities.order_submission_eligible),
    }))
}

const fn action_decision(
    permission_granted: bool,
    capability: CapabilityView,
) -> ActionEligibilityDecision {
    ActionEligibilityDecision {
        enabled: permission_granted && capability.enabled,
        permission_granted,
        capability,
    }
}

pub async fn activate_bootstrap(
    state: web::Data<AppState>,
    actor: AuthedActor,
    ActingRole(acting_role): ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<ActivateBootstrapRequest>,
) -> Result<WebResponse<BootstrapView>, WebError> {
    let request = body.into_inner();
    op_ctx.set_action(OperationCategory::System, "system.bootstrap.activate");
    op_ctx.set_detail(serde_json::json!({
        "bootstrap_contract_version": request.bootstrap_contract_version,
        "expected_state_revision": request.expected_state_revision,
        "reason": &request.reason,
        "report_only_forced_ack": request.report_only_forced_ack,
    }))?;
    let view = state
        .bootstrap
        .activate(request, &actor.claims.username, &acting_role)
        .await?;
    Ok(WebResponse::ok(view))
}

pub async fn health(state: web::Data<AppState>) -> Result<WebResponse<HealthReport>, WebError> {
    Ok(WebResponse::ok(state.control.health().await))
}

pub async fn execution_recovery(
    state: web::Data<AppState>,
) -> Result<WebResponse<ExecutionRecoveryView>, WebError> {
    Ok(WebResponse::ok(state.execution_recovery.view().await?))
}

pub async fn quant_mode(
    state: web::Data<AppState>,
) -> Result<WebResponse<QuantModeView>, WebError> {
    Ok(WebResponse::ok(QuantModeView {
        mode: state.control.quant_runtime_mode(),
    }))
}

pub async fn switch_quant_mode(
    state: web::Data<AppState>,
    actor: AuthedActor,
    _acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<SwitchQuantModeRequest>,
) -> Result<WebResponse<QuantModeTransitionReport>, WebError> {
    let body = body.into_inner();
    let before_hash = canonical_state_hash(&QuantModeView {
        mode: state.control.quant_runtime_mode(),
    })?;
    op_ctx.set_action(OperationCategory::System, "system.switch_quant_mode");
    op_ctx.set_detail(serde_json::json!({
        "target_mode": body.mode.as_str(),
        "reason": body.reason,
    }))?;
    let report = state
        .control
        .switch_quant_mode(body.mode, &actor.claims.username, &body.reason)
        .await?;
    let after_hash = canonical_state_hash(&QuantModeView {
        mode: state.control.quant_runtime_mode(),
    })?;
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    Ok(WebResponse::ok(report))
}

pub async fn kill_switch_status(
    state: web::Data<AppState>,
) -> Result<WebResponse<KillSwitchView>, WebError> {
    Ok(WebResponse::ok(state.kill_switch.view()))
}

/// The permission a kill-switch transition requires, keyed on the transition's
/// intent rather than a single blanket `system:halt`:
///
/// - entering or clearing `emergency_halted` is an emergency-authority action →
///   `system:emergency`;
/// - loosening (a strictly lower restriction rank), e.g. clearing a tightened
///   state back toward `closed`, is a resume action → `system:resume`;
/// - tightening or holding at an equal rank is a halt action → `system:halt`.
const fn required_kill_switch_op(current: KillSwitchState, target: KillSwitchState) -> Operation {
    if target.is_emergency() || current.is_emergency() {
        Operation::Emergency
    } else if target.restriction_rank() < current.restriction_rank() {
        Operation::Resume
    } else {
        Operation::Halt
    }
}

pub async fn set_kill_switch(
    state: web::Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<SetKillSwitchRequest>,
) -> Result<WebResponse<KillSwitchView>, WebError> {
    let body = body.into_inner();
    let current = state.kill_switch.view();
    // Operation-level authorization deferred from the route rule: the required
    // system permission depends on the requested transition. Super-admin keeps
    // the global bypass; every other actor's declared role must carry the op.
    let required_op = required_kill_switch_op(current.state, body.state);
    if !actor.roles.contains_enabled(SUPER_ADMIN_ROLE)
        && !state
            .casbin
            .has_policy(
                &acting_role.0,
                ResourceType::System.as_str(),
                required_op.as_str(),
            )
            .await
    {
        return Err(WebError::Forbidden);
    }
    let before_hash = canonical_state_hash(&current)?;
    op_ctx.set_action(OperationCategory::System, "system.set_kill_switch");
    op_ctx.set_detail(serde_json::json!({
        "target_state": body.state.as_str(),
        "reason": body.reason,
        "ack": body.ack,
    }))?;
    let view = state
        .kill_switch
        .set(SetKillSwitchCommand {
            target: body.state,
            actor: actor.claims.username.clone(),
            reason: body.reason,
            ack: body.ack,
            // Operator-initiated sets are self-clearable; only automated
            // breaker escalation latches (`emergency_halted` latches regardless).
            latch: false,
        })
        .await?;
    let after_hash = canonical_state_hash(&view)?;
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    Ok(WebResponse::ok(view))
}

fn canonical_state_hash<T: Serialize>(state: &T) -> Result<ContentHash, WebError> {
    CanonicalDigest::content_hash_json(state)
        .map_err(|error| WebError::Internal(format!("canonical state hash failed: {error}")))
}
