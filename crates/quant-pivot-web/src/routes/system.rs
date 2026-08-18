//! System status + quant runtime mode endpoints.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use chrono::Utc;
use quant_pivot_models::{
    domain::{
        api::{
            ActionEligibilityDecision, ActionEligibilityView, CapabilityView,
            ExecutionRecoveryView, FreshBootProfileProgressView, FreshBootProgressView,
            FreshBootRunDetailView, FreshBootRunEventView, FreshBootRunProgressView,
            RetryFreshBootRunRequest, SetEntryAuthorizationPolicyRequest, SetKillSwitchRequest,
            SupersedeFreshBootRunRequest, SwitchSettlementWritePolicyRequest, SystemStatusView,
            system::{
                ExchangeHistoryQuarantinePageView, ExchangeHistoryQuarantineQuery,
                ExchangeHistoryQuarantineView, FreshBootCapabilitySummary,
            },
        },
        data_plane::{
            ExchangeHistoryFrontier, ExchangeHistoryFrontierProgress, ExchangeHistoryQuarantineRead,
        },
        governance::{HealthReport, KillSwitchView, RuntimeControlSnapshot},
        ports::{EntryAuthorizationTransitionReport, SetKillSwitchCommand},
        quant::{FreshBootRunContract, SupersedeFreshBootRun},
    },
    enums::{
        execution::KillSwitchState,
        operation_log::OperationCategory,
        quant::{FreshBootBlockedReason, FreshBootStatus},
        rbac::{Operation, ResourceType},
    },
    hashing::CanonicalDigest,
    types::{ContentHash, FreshBootRunId},
};
use serde::Serialize;

use crate::{
    audit::OperationCtx,
    auth::casbin::{Rule, checker::SUPER_ADMIN_ROLE},
    error::WebError,
    extractors::{ActingRole, AuthedActor, RequestId, ValidatedJson},
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
            "/system/exchange-history",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            exchange_history,
        ),
        spec(
            Method::GET,
            "/system/exchange-history/quarantines",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            exchange_history_quarantines,
        ),
        spec(
            Method::GET,
            "/system/fresh-boot",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            fresh_boot,
        ),
        spec(
            Method::GET,
            "/system/fresh-boot/{run_id}",
            Rule::ResourceOp(ResourceType::System, Operation::Read),
            fresh_boot_run,
        ),
        spec(
            Method::POST,
            "/system/fresh-boot/{run_id}/retry-now",
            Rule::ActingRoleGoverned(ResourceType::System, Operation::Update),
            retry_fresh_boot,
        ),
        spec(
            Method::POST,
            "/system/fresh-boot/{run_id}/supersede",
            Rule::ActingRoleGoverned(ResourceType::System, Operation::Resolve),
            supersede_fresh_boot,
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
            "/system/runtime-controls/entry-authorization-policy",
            Rule::ActingRoleGoverned(ResourceType::System, Operation::SwitchMode),
            switch_entry_authorization_policy,
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

pub async fn exchange_history(
    state: Data<AppState>,
) -> WebResponse<ExchangeHistoryFrontierProgress> {
    WebResponse::ok(state.exchange_history_progress.snapshot())
}

pub async fn exchange_history_quarantines(
    state: Data<AppState>,
    query: Query<ExchangeHistoryQuarantineQuery>,
) -> Result<WebResponse<ExchangeHistoryQuarantinePageView>, WebError> {
    let query = query.into_inner();
    let limit = query.normalized_limit().ok_or_else(|| {
        WebError::BadRequest(format!(
            "limit must be between 1 and {}",
            ExchangeHistoryQuarantineQuery::MAX_LIMIT
        ))
    })?;
    let fetch_limit = limit
        .checked_add(1)
        .ok_or_else(|| WebError::BadRequest("quarantine page limit overflowed".to_owned()))?;
    let mut records = state
        .exchange_history
        .page_quarantine(ExchangeHistoryQuarantineRead {
            status: query.status,
            frontier: query.frontier,
            kind: query.kind,
            after: query.after,
            limit: fetch_limit,
        })
        .await?;
    let has_more = records.len()
        > usize::try_from(limit)
            .map_err(|error| WebError::BadRequest(format!("invalid page limit: {error}")))?;
    records.truncate(
        usize::try_from(limit)
            .map_err(|error| WebError::BadRequest(format!("invalid page limit: {error}")))?,
    );
    let items = records
        .into_iter()
        .map(ExchangeHistoryQuarantineView::from)
        .collect::<Vec<_>>();
    let next_after = has_more
        .then(|| items.last().map(|item| item.quarantine_id))
        .flatten();
    Ok(WebResponse::ok(ExchangeHistoryQuarantinePageView {
        items,
        next_after,
    }))
}

pub async fn fresh_boot(
    state: Data<AppState>,
) -> Result<WebResponse<FreshBootProgressView>, WebError> {
    let mut runs = state.fresh_boot_runs.list_latest().await?;
    runs.sort_unstable_by_key(|run| run.route);
    let mut profiles = Vec::with_capacity(runs.len());
    for run in runs {
        let last_event = state
            .fresh_boot_runs
            .list_events(run.run_id)
            .await?
            .pop()
            .map(FreshBootRunEventView::from);
        let training = match run.training_dataset_id {
            Some(id) => state.fresh_boot_datasets.find_by_id(&id).await?,
            None => None,
        };
        let calibration = match run.calibration_dataset_id {
            Some(id) => state.fresh_boot_datasets.find_by_id(&id).await?,
            None => None,
        };
        profiles.push(FreshBootProfileProgressView {
            run: run.into(),
            last_event,
            training_dataset_status: training.as_ref().map(|dataset| dataset.status),
            training_sample_count: training.and_then(|dataset| dataset.sample_count),
            calibration_dataset_status: calibration.as_ref().map(|dataset| dataset.status),
            calibration_sample_count: calibration.and_then(|dataset| dataset.sample_count),
        });
    }
    let exchange_history = state.exchange_history_progress.snapshot();
    let capability = FreshBootCapabilitySummary::from_profiles(&exchange_history, &profiles);
    Ok(WebResponse::ok(FreshBootProgressView {
        observed_at: Utc::now(),
        exchange_history,
        capability,
        profiles,
    }))
}

pub async fn fresh_boot_run(
    state: Data<AppState>,
    run_id: Path<FreshBootRunId>,
) -> Result<WebResponse<FreshBootRunDetailView>, WebError> {
    let run_id = run_id.into_inner();
    let run = state
        .fresh_boot_runs
        .find(&run_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("fresh-boot run not found: {run_id}")))?;
    let events = state
        .fresh_boot_runs
        .list_events(run_id)
        .await?
        .into_iter()
        .map(FreshBootRunEventView::from)
        .collect();
    Ok(WebResponse::ok(FreshBootRunDetailView {
        run: FreshBootRunProgressView::from(run),
        events,
    }))
}

pub async fn retry_fresh_boot(
    state: Data<AppState>,
    run_id: Path<FreshBootRunId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RetryFreshBootRunRequest>,
) -> Result<WebResponse<FreshBootRunProgressView>, WebError> {
    let run_id = run_id.into_inner();
    let request = body.into_inner();
    let now = Utc::now();
    let run = state
        .fresh_boot_runs
        .retry_now(
            run_id,
            request.expected_revision,
            actor.claims.username.clone(),
            request.reason.clone(),
            now,
        )
        .await?;
    op_ctx.set_action(OperationCategory::System, "system.fresh_boot.retry_now");
    op_ctx.set_resource(ResourceType::System, run_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "run_id": run_id,
        "expected_revision": request.expected_revision,
        "reason": request.reason,
        "actor": actor.claims.username,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }))?;
    Ok(WebResponse::accepted(FreshBootRunProgressView::from(run)))
}

pub async fn supersede_fresh_boot(
    state: Data<AppState>,
    run_id: Path<FreshBootRunId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<SupersedeFreshBootRunRequest>,
) -> Result<WebResponse<FreshBootRunProgressView>, WebError> {
    let run_id = run_id.into_inner();
    let request = body.into_inner();
    let blocked = state
        .fresh_boot_runs
        .find(&run_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("fresh-boot run not found: {run_id}")))?;
    if blocked.status != FreshBootStatus::BlockedTerminal {
        return Err(WebError::Conflict(
            "only a terminal fresh-boot blocker can be superseded".to_owned(),
        ));
    }
    let plan = state
        .exchange_history
        .load_plan(137)
        .await?
        .ok_or_else(|| WebError::Conflict("exchange-history plan is unavailable".to_owned()))?;
    let from_block = plan.required_from_block(blocked.route);
    if matches!(
        blocked.blocked_reason,
        Some(
            FreshBootBlockedReason::ProviderMismatch
                | FreshBootBlockedReason::UnknownToken
                | FreshBootBlockedReason::DecodeFailure
                | FreshBootBlockedReason::HistoryQuarantined
                | FreshBootBlockedReason::SourceCoverageInvalid
        )
    ) {
        let (activation_blockers, retention_blockers) = tokio::try_join!(
            state.exchange_history.active_quarantine(
                ExchangeHistoryFrontier::Activation,
                from_block,
                plan.activation_through_block,
                1,
            ),
            state.exchange_history.active_quarantine(
                ExchangeHistoryFrontier::Retention,
                from_block,
                plan.activation_through_block,
                1,
            ),
        )?;
        if !activation_blockers.is_empty() || !retention_blockers.is_empty() {
            return Err(WebError::Conflict(
                "the overlapping exchange-history blocker is still active".to_owned(),
            ));
        }
    }
    let bundle = state
        .runtime_config
        .load_current_bundle()
        .await?
        .ok_or_else(|| WebError::Conflict("active policy bundle is unavailable".to_owned()))?;
    let now = Utc::now();
    let replacement = FreshBootRunContract {
        profile_ref: blocked.research_profile_artifact_id.profile_ref(),
        route: blocked.route,
        history_plan_id: plan.plan_id,
        history_policy_hash: plan.policy_hash,
        history_from_block: from_block,
        history_through_block: plan.activation_through_block,
        decision_policy_snapshot_id: bundle.decision_policy_snapshot_id,
        decision_policy_snapshot_hash: bundle.snapshot_hash,
        supersedes_run_id: Some(blocked.run_id),
    }
    .seal(plan.created_at, now)?;
    let replacement_run_id = replacement.run_id;
    let run = state
        .fresh_boot_runs
        .supersede(
            SupersedeFreshBootRun {
                run_id,
                expected_revision: request.expected_revision,
                replacement_run_id,
                reason: request.reason.clone(),
                actor: actor.claims.username.clone(),
                occurred_at: now,
            },
            replacement,
        )
        .await?;
    op_ctx.set_action(OperationCategory::System, "system.fresh_boot.supersede");
    op_ctx.set_resource(ResourceType::System, run_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "run_id": run_id,
        "replacement_run_id": replacement_run_id,
        "expected_revision": request.expected_revision,
        "reason": request.reason,
        "actor": actor.claims.username,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }))?;
    Ok(WebResponse::accepted(FreshBootRunProgressView::from(run)))
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

pub async fn switch_entry_authorization_policy(
    state: Data<AppState>,
    actor: AuthedActor,
    _acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<SetEntryAuthorizationPolicyRequest>,
) -> Result<WebResponse<EntryAuthorizationTransitionReport>, WebError> {
    let body = body.into_inner();
    let before_hash = canonical_state_hash(&state.control.snapshot())?;
    op_ctx.set_action(
        OperationCategory::System,
        "system.switch_entry_authorization_policy",
    );
    op_ctx.set_detail(serde_json::json!({
        "target_policy": body.policy.as_str(),
        "reason": &body.reason,
        "expected_revision": body.expected_revision,
    }))?;
    let report = state
        .control
        .switch_entry_authorization_policy(
            body.policy,
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
