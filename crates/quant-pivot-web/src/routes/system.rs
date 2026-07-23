//! System status + quant runtime mode endpoints.

use actix_web::{http::Method, web::Data};
use quant_pivot_models::{
    domain::{
        api::{
            ActionEligibilityDecision, ActionEligibilityView, CapabilityView,
            ExecutionRecoveryView, SetKillSwitchRequest, SwitchQuantModeRequest,
            SwitchSettlementWritePolicyRequest, SystemStatusView,
        },
        governance::{HealthReport, KillSwitchView, RuntimeControlSnapshot},
        ports::{QuantModeTransitionReport, SetKillSwitchCommand},
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
            Method::GET,
            "/system/health",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            health,
        ),
        spec(
            Method::GET,
            "/system/runtime-controls",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            runtime_controls,
        ),
        spec(
            Method::POST,
            "/system/runtime-controls/quant-mode",
            Rule::ActingRoleGoverned(ResourceType::System, Operation::SwitchMode),
            switch_quant_mode,
        ),
        spec(
            Method::POST,
            "/system/runtime-controls/settlement-write-policy",
            Rule::ActingRoleGoverned(ResourceType::System, Operation::SwitchMode),
            switch_settlement_write_policy,
        ),
        spec(
            Method::POST,
            "/system/runtime-controls/kill-switch",
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

pub async fn status(state: Data<AppState>) -> Result<WebResponse<SystemStatusView>, WebError> {
    let runtime = state.control.system_status();
    let capabilities = state.capabilities.capabilities(&runtime).await?;
    Ok(WebResponse::ok(SystemStatusView {
        runtime,
        capabilities,
    }))
}

pub async fn action_eligibility(
    state: Data<AppState>,
    actor: AuthedActor,
) -> Result<WebResponse<ActionEligibilityView>, WebError> {
    let runtime = state.control.system_status();
    let capabilities = state.capabilities.capabilities(&runtime).await?;
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

pub async fn health(state: Data<AppState>) -> Result<WebResponse<HealthReport>, WebError> {
    Ok(WebResponse::ok(state.control.health().await))
}

pub async fn execution_recovery(
    state: Data<AppState>,
) -> Result<WebResponse<ExecutionRecoveryView>, WebError> {
    Ok(WebResponse::ok(state.execution_recovery.view().await?))
}

pub async fn runtime_controls(
    state: Data<AppState>,
) -> Result<WebResponse<RuntimeControlSnapshot>, WebError> {
    Ok(WebResponse::ok(state.control.snapshot()))
}

pub async fn switch_quant_mode(
    state: Data<AppState>,
    actor: AuthedActor,
    _acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<SwitchQuantModeRequest>,
) -> Result<WebResponse<QuantModeTransitionReport>, WebError> {
    let body = body.into_inner();
    let before_hash = canonical_state_hash(&state.control.snapshot())?;
    op_ctx.set_action(OperationCategory::System, "system.switch_quant_mode");
    op_ctx.set_detail(serde_json::json!({
        "target_mode": body.mode.as_str(),
        "reason": &body.reason,
        "expected_revision": body.expected_revision,
    }))?;
    let report = state
        .control
        .switch_quant_mode(
            body.mode,
            body.expected_revision,
            &actor.claims.username,
            &body.reason,
        )
        .await?;
    let after_hash = canonical_state_hash(&state.control.snapshot())?;
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    Ok(WebResponse::ok(report))
}

pub async fn switch_settlement_write_policy(
    state: Data<AppState>,
    actor: AuthedActor,
    _acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<SwitchSettlementWritePolicyRequest>,
) -> Result<WebResponse<RuntimeControlSnapshot>, WebError> {
    let body = body.into_inner();
    let before = state.control.snapshot();
    let before_hash = canonical_state_hash(&before)?;
    op_ctx.set_action(
        OperationCategory::System,
        "system.switch_settlement_write_policy",
    );
    op_ctx.set_detail(serde_json::json!({
        "target_policy": body.policy.as_str(),
        "reason": &body.reason,
        "expected_revision": body.expected_revision,
    }))?;
    let snapshot = state
        .control
        .switch_settlement_write_policy(
            body.policy,
            body.expected_revision,
            &actor.claims.username,
            &body.reason,
        )
        .await?;
    let after_hash = canonical_state_hash(&snapshot)?;
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    Ok(WebResponse::ok(snapshot))
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
    state: Data<AppState>,
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
        "reason": &body.reason,
        "ack": body.ack,
    }))?;
    let view = state
        .kill_switch
        .set(SetKillSwitchCommand {
            expected_revision: body.expected_revision,
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
