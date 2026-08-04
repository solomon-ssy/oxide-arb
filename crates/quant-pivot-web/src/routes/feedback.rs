//! Protected feedback-cycle and promotion-permit endpoints.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::{
        api::{
            ActivateModelRouteRequest, BootstrapModelRouteRequest, CancelFeedbackCycleRequest,
            DriftReportListQuery, DriftReportView, FeedbackCycleDetailView, FeedbackCycleListQuery,
            FeedbackCycleMutationView, FeedbackCycleTriggerRequest, FeedbackCycleTriggerView,
            FeedbackCycleView, FeedbackOverviewView, FeedbackSchedulerControlRequest,
            FeedbackSchedulerListView, FeedbackSchedulerMutationView, IssuePromotionPermitRequest,
            ModelRouteActivationMutationView, ModelRouteActivationReceiptView,
            ModelRouteBootstrapReceiptView, PromotionPermitListQuery, PromotionPermitMutationView,
            PromotionPermitView, RejectShadowBindingRequest, RemediateResolutionProjectionRequest,
            ResolutionProjectionRemediationView, RevokePromotionPermitRequest,
            ShadowBindingRejectionReceiptView,
        },
        pagination::Paginated,
        quant::FeedbackCycleActor,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, FeedbackCycleId, PolicyActivationId, PromotionPermitId, ResearchProfileId,
        ResolutionObservationId, RoleCode, ShadowBindingArtifactId,
    },
};
use serde::Serialize;

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, AuthedActor, RequestId, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Feedback workbench routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/research/feedback-overview",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            overview,
        ),
        spec(
            Method::GET,
            "/research/feedback-cycles",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list_cycles,
        ),
        spec(
            Method::POST,
            "/research/feedback-cycles",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            trigger_cycle,
        ),
        spec(
            Method::GET,
            "/research/feedback-cycles/{cycle_id}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            get_cycle,
        ),
        spec(
            Method::POST,
            "/research/feedback-cycles/{cycle_id}/cancel",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            cancel_cycle,
        ),
        spec(
            Method::GET,
            "/research/drift-reports",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list_drift_reports,
        ),
        spec(
            Method::GET,
            "/research/feedback-schedulers",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list_schedulers,
        ),
        spec(
            Method::POST,
            "/research/feedback-schedulers/{profile_id}/pause",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Update),
            pause_scheduler,
        ),
        spec(
            Method::POST,
            "/research/feedback-schedulers/{profile_id}/resume",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Update),
            resume_scheduler,
        ),
        spec(
            Method::GET,
            "/research/model-route-activation-permits",
            Rule::ResourceOp(ResourceType::Publication, Operation::Read),
            list_permits,
        ),
        spec(
            Method::POST,
            "/research/model-route-activation-permits",
            Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Authorize),
            issue_permit,
        ),
        spec(
            Method::POST,
            "/research/model-route-activation-permits/{permit_id}/revoke",
            Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Retire),
            revoke_permit,
        ),
        spec(
            Method::POST,
            "/research/model-route-bootstraps",
            Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Publish),
            bootstrap_route,
        ),
        spec(
            Method::POST,
            "/research/model-route-activations",
            Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Activate),
            activate_route,
        ),
        spec(
            Method::POST,
            "/research/model-route-shadow-bindings/{binding_id}/reject",
            Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Reject),
            reject_shadow,
        ),
        spec(
            Method::POST,
            "/research/resolution-projections/{observation_id}/remediations",
            Rule::ActingRoleGoverned(ResourceType::Reconciliation, Operation::Resolve),
            remediate_resolution,
        ),
        spec(
            Method::GET,
            "/research/model-route-activations/{activation_id}",
            Rule::ResourceOp(ResourceType::Publication, Operation::Read),
            get_activation,
        ),
    ]
}

/// `GET /api/research/feedback-overview`.
pub async fn overview(
    state: Data<AppState>,
) -> Result<WebResponse<FeedbackOverviewView>, WebError> {
    Ok(WebResponse::ok(state.feedback_read.overview().await?))
}

/// `GET /api/research/feedback-cycles`.
pub async fn list_cycles(
    state: Data<AppState>,
    query: Query<FeedbackCycleListQuery>,
) -> Result<WebResponse<Paginated<FeedbackCycleView>>, WebError> {
    let page = state.feedback_read.list_cycles(query.into_inner()).await?;
    Ok(WebResponse::ok(page))
}

/// `GET /api/research/feedback-cycles/{cycle_id}`.
pub async fn get_cycle(
    state: Data<AppState>,
    cycle_id: Path<FeedbackCycleId>,
) -> Result<WebResponse<FeedbackCycleDetailView>, WebError> {
    let cycle_id = cycle_id.into_inner();
    let view = state
        .feedback_read
        .get_cycle(&cycle_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("feedback cycle not found: {cycle_id}")))?;
    Ok(WebResponse::ok(view))
}

/// `GET /api/research/drift-reports`.
pub async fn list_drift_reports(
    state: Data<AppState>,
    query: Query<DriftReportListQuery>,
) -> Result<WebResponse<Paginated<DriftReportView>>, WebError> {
    let page = state
        .feedback_read
        .list_drift_reports(query.into_inner())
        .await?;
    Ok(WebResponse::ok(page))
}

/// `GET /api/research/feedback-schedulers`.
pub async fn list_schedulers(
    state: Data<AppState>,
) -> Result<WebResponse<FeedbackSchedulerListView>, WebError> {
    Ok(WebResponse::ok(
        state.feedback_read.list_schedulers().await?,
    ))
}

/// `POST /api/research/feedback-schedulers/{profile_id}/pause`.
pub async fn pause_scheduler(
    state: Data<AppState>,
    profile_id: Path<ResearchProfileId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<FeedbackSchedulerControlRequest>,
) -> Result<WebResponse<FeedbackSchedulerMutationView>, WebError> {
    let profile_id = profile_id.into_inner();
    let view = state
        .feedback_mutation
        .control_scheduler(profile_id.clone(), true, body.into_inner())
        .await?;
    let after_hash = canonical_state_hash(&view.state)?;
    op_ctx.set_action(OperationCategory::Governance, "feedback.scheduler.pause");
    op_ctx.set_resource(ResourceType::Materialization, profile_id.to_string());
    op_ctx.set_state_hashes(None, Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "profile_id": profile_id.to_string(),
        "paused": true,
        "pause_revision": view.state.pause_revision,
        "actor": actor.claims.username,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }))?;
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/feedback-schedulers/{profile_id}/resume`.
pub async fn resume_scheduler(
    state: Data<AppState>,
    profile_id: Path<ResearchProfileId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<FeedbackSchedulerControlRequest>,
) -> Result<WebResponse<FeedbackSchedulerMutationView>, WebError> {
    let profile_id = profile_id.into_inner();
    let view = state
        .feedback_mutation
        .control_scheduler(profile_id.clone(), false, body.into_inner())
        .await?;
    let after_hash = canonical_state_hash(&view.state)?;
    op_ctx.set_action(OperationCategory::Governance, "feedback.scheduler.resume");
    op_ctx.set_resource(ResourceType::Materialization, profile_id.to_string());
    op_ctx.set_state_hashes(None, Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "profile_id": profile_id.to_string(),
        "paused": false,
        "pause_revision": view.state.pause_revision,
        "actor": actor.claims.username,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }))?;
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/feedback-cycles`.
pub async fn trigger_cycle(
    state: Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<FeedbackCycleTriggerRequest>,
) -> Result<WebResponse<FeedbackCycleTriggerView>, WebError> {
    let request = body.into_inner();
    let profile_id = request.profile_id.clone();
    let view = state
        .feedback_mutation
        .trigger_cycle(request, feedback_actor(&actor, &acting_role)?)
        .await?;
    let cycle_id = view.cycle.feedback_cycle_id;
    let after_hash = canonical_state_hash(&view.cycle)?;
    op_ctx.set_action(OperationCategory::Governance, "feedback.cycle.trigger");
    op_ctx.set_resource(ResourceType::Materialization, cycle_id.to_string());
    op_ctx.set_state_hashes(None, Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "feedback_cycle_id": cycle_id.to_string(),
        "profile_id": profile_id.to_string(),
        "status": view.cycle.status,
        "cycle_reused": view.cycle_reused,
        "trigger_replayed": view.trigger_replayed,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }))?;
    Ok(WebResponse::accepted(view))
}

/// `POST /api/research/feedback-cycles/{cycle_id}/cancel`.
pub async fn cancel_cycle(
    state: Data<AppState>,
    cycle_id: Path<FeedbackCycleId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<CancelFeedbackCycleRequest>,
) -> Result<WebResponse<FeedbackCycleMutationView>, WebError> {
    let cycle_id = cycle_id.into_inner();
    let view = state
        .feedback_mutation
        .cancel_cycle(
            cycle_id,
            body.into_inner(),
            feedback_actor(&actor, &acting_role)?,
        )
        .await?;
    let after_hash = canonical_state_hash(&view.cycle)?;
    op_ctx.set_action(OperationCategory::Governance, "feedback.cycle.cancel");
    op_ctx.set_resource(ResourceType::Materialization, cycle_id.to_string());
    op_ctx.set_state_hashes(None, Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "feedback_cycle_id": cycle_id.to_string(),
        "status": view.cycle.status,
        "replayed": view.replayed,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }))?;
    Ok(WebResponse::accepted(view))
}

/// `GET /api/research/model-route-activation-permits`.
pub async fn list_permits(
    state: Data<AppState>,
    query: Query<PromotionPermitListQuery>,
) -> Result<WebResponse<Paginated<PromotionPermitView>>, WebError> {
    let page = state
        .feedback_mutation
        .list_permits(query.into_inner())
        .await?;
    Ok(WebResponse::ok(page))
}

/// `POST /api/research/model-route-activation-permits`.
pub async fn issue_permit(
    state: Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<IssuePromotionPermitRequest>,
) -> Result<WebResponse<PromotionPermitMutationView>, WebError> {
    let request = body.into_inner();
    let feedback_cycle_id = request.feedback_cycle_id;
    let view = state
        .feedback_mutation
        .issue_permit(request, feedback_actor(&actor, &acting_role)?)
        .await?;
    let permit_id = view.permit.promotion_permit_id;
    let after_hash = canonical_state_hash(&view.permit)?;
    op_ctx.set_action(OperationCategory::Governance, "feedback.permit.issue");
    op_ctx.set_resource(ResourceType::Publication, permit_id.to_string());
    op_ctx.set_state_hashes(None, Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "promotion_permit_id": permit_id.to_string(),
        "feedback_cycle_id": feedback_cycle_id.to_string(),
        "status": view.permit.status,
        "replayed": view.replayed,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }))?;
    Ok(WebResponse::created(view))
}

/// `POST /api/research/model-route-activation-permits/{permit_id}/revoke`.
pub async fn revoke_permit(
    state: Data<AppState>,
    permit_id: Path<PromotionPermitId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RevokePromotionPermitRequest>,
) -> Result<WebResponse<PromotionPermitMutationView>, WebError> {
    let permit_id = permit_id.into_inner();
    let view = state
        .feedback_mutation
        .revoke_permit(
            permit_id,
            body.into_inner(),
            feedback_actor(&actor, &acting_role)?,
        )
        .await?;
    let after_hash = canonical_state_hash(&view.permit)?;
    op_ctx.set_action(OperationCategory::Governance, "feedback.permit.revoke");
    op_ctx.set_resource(ResourceType::Publication, permit_id.to_string());
    op_ctx.set_state_hashes(None, Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "promotion_permit_id": permit_id.to_string(),
        "revision": view.permit.revision,
        "status": view.permit.status,
        "replayed": view.replayed,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }))?;
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/model-route-bootstraps`.
pub async fn bootstrap_route(
    state: Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<BootstrapModelRouteRequest>,
) -> Result<WebResponse<ModelRouteBootstrapReceiptView>, WebError> {
    let request = body.into_inner();
    let model_version_id = request.model_version_id;
    let view = state
        .feedback_mutation
        .bootstrap_route(request, feedback_actor(&actor, &acting_role)?)
        .await?;
    let after_hash = canonical_state_hash(&view)?;
    op_ctx.set_action(OperationCategory::Governance, "model_route.bootstrap");
    op_ctx.set_resource(
        ResourceType::Publication,
        view.policy_activation_id.to_string(),
    );
    op_ctx.set_state_hashes(None, Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "model_version_id": model_version_id.to_string(),
        "route": view.route,
        "previous_route_generation": view.previous_route_generation,
        "activated_route_generation": view.activated_route_generation,
        "transaction_hash": view.transaction_hash,
        "replayed": view.replayed,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }))?;
    Ok(WebResponse::created(view))
}

/// `POST /api/research/model-route-activations`.
pub async fn activate_route(
    state: Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<ActivateModelRouteRequest>,
) -> Result<WebResponse<ModelRouteActivationMutationView>, WebError> {
    let request = body.into_inner();
    let permit_id = request.promotion_permit_id;
    let cycle_id = request.feedback_cycle_id;
    let view = state
        .feedback_mutation
        .activate_route(request, feedback_actor(&actor, &acting_role)?)
        .await?;
    let after_hash = canonical_state_hash(&view)?;
    op_ctx.set_action(OperationCategory::Governance, "model_route.activate");
    op_ctx.set_resource(
        ResourceType::Publication,
        view.receipt.policy_activation_id.to_string(),
    );
    op_ctx.set_state_hashes(None, Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "promotion_permit_id": permit_id.to_string(),
        "feedback_cycle_id": cycle_id.to_string(),
        "previous_route_generation": view.receipt.previous_route_generation,
        "activated_route_generation": view.receipt.activated_route_generation,
        "transaction_hash": view.receipt.transaction_hash,
        "replayed": view.replayed,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }))?;
    Ok(WebResponse::created(view))
}

/// `POST /api/research/model-route-shadow-bindings/{binding_id}/reject`.
pub async fn reject_shadow(
    state: Data<AppState>,
    binding_id: Path<ShadowBindingArtifactId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RejectShadowBindingRequest>,
) -> Result<WebResponse<ShadowBindingRejectionReceiptView>, WebError> {
    let binding_id = binding_id.into_inner();
    let view = state
        .feedback_mutation
        .reject_shadow(
            binding_id,
            body.into_inner(),
            feedback_actor(&actor, &acting_role)?,
        )
        .await?;
    let after_hash = canonical_state_hash(&view)?;
    op_ctx.set_action(OperationCategory::Governance, "model_route.shadow.reject");
    op_ctx.set_resource(ResourceType::Publication, binding_id.to_string());
    op_ctx.set_state_hashes(None, Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "binding_id": binding_id.to_string(),
        "feedback_cycle_id": view.receipt.feedback_cycle_id.to_string(),
        "route": view.receipt.route,
        "previous_binding_generation": view.receipt.previous_binding_generation,
        "cleared_route_generation": view.receipt.cleared_route_generation,
        "policy_activation_id": view.receipt.policy_activation_id.to_string(),
        "replayed": view.replayed,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }))?;
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/resolution-projections/{observation_id}/remediations`.
pub async fn remediate_resolution(
    state: Data<AppState>,
    observation_id: Path<ResolutionObservationId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RemediateResolutionProjectionRequest>,
) -> Result<WebResponse<ResolutionProjectionRemediationView>, WebError> {
    let observation_id = observation_id.into_inner();
    let view = state
        .feedback_mutation
        .remediate_resolution(
            observation_id,
            body.into_inner(),
            feedback_actor(&actor, &acting_role)?,
        )
        .await?;
    let after_hash = canonical_state_hash(&view)?;
    op_ctx.set_action(
        OperationCategory::Governance,
        "resolution_projection.remediate",
    );
    op_ctx.set_resource(ResourceType::Reconciliation, observation_id.to_string());
    op_ctx.set_state_hashes(None, Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "resolution_observation_id": observation_id.to_string(),
        "remediation_id": view.remediation.remediation_id.to_string(),
        "action": view.remediation.action,
        "expected_revision": view.remediation.expected_revision,
        "committed_revision": view.remediation.committed_revision,
        "prior_status": view.remediation.prior_status,
        "resulting_status": view.remediation.resulting_status,
        "replayed": view.replayed,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }))?;
    Ok(WebResponse::ok(view))
}

/// `GET /api/research/model-route-activations/{activation_id}`.
pub async fn get_activation(
    state: Data<AppState>,
    activation_id: Path<PolicyActivationId>,
) -> Result<WebResponse<ModelRouteActivationReceiptView>, WebError> {
    let activation_id = activation_id.into_inner();
    let view = state
        .feedback_mutation
        .get_activation(activation_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!("model-route activation not found: {activation_id}"))
        })?;
    Ok(WebResponse::ok(view))
}

fn feedback_actor(
    actor: &AuthedActor,
    acting_role: &ActingRole,
) -> Result<FeedbackCycleActor, WebError> {
    let user_id = actor.user_id().map_err(|error| {
        WebError::Internal(format!("authenticated subject is invalid: {error}"))
    })?;
    Ok(FeedbackCycleActor {
        user_id,
        acting_role: RoleCode::new(acting_role.0.clone()),
    })
}

fn canonical_state_hash<T: Serialize>(state: &T) -> Result<ContentHash, WebError> {
    CanonicalDigest::content_hash_json(state)
        .map_err(|error| WebError::Internal(format!("canonical state hash failed: {error}")))
}
