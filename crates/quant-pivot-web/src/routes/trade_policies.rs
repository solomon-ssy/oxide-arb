//! Governed trade-policy artifact endpoints (Phase 11.7).

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        FitTradePolicyRequest, JobSubmitContext, Paginated, ResearchJobView,
        TradePolicyAuditListQuery, TradePolicyDetailView, TradePolicyFitPreflightRequest,
        TradePolicyFitPreflightView, TradePolicyGovernanceAuditView, TradePolicyGovernanceRequest,
        TradePolicyListQuery, TradePolicyPreflightCheckStatus, TradePolicySummaryView,
    },
    enums::{
        operation_log::OperationCategory,
        quant::{ResearchJobKind, TradePolicyStatus},
        rbac::{Operation, ResourceType},
    },
    types::{ResearchJobId, TradePolicyArtifactId},
};
use uuid::Uuid;

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
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
            "/research/trade-policies",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list,
        ),
        spec(
            Method::POST,
            "/research/trade-policy-fits/preflight",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            preflight,
        ),
        spec(
            Method::POST,
            "/research/trade-policy-fits",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            fit,
        ),
        spec(
            Method::GET,
            "/research/trade-policy-fits/{id}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            get_fit,
        ),
        spec(
            Method::GET,
            "/research/trade-policies/{id}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            get,
        ),
        spec(
            Method::GET,
            "/research/trade-policies/{id}/audits",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            audits,
        ),
        spec(
            Method::POST,
            "/research/trade-policies/{id}/validate",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            validate,
        ),
        spec(
            Method::POST,
            "/research/trade-policies/{id}/publish",
            Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Publish),
            publish,
        ),
        spec(
            Method::POST,
            "/research/trade-policies/{id}/retire",
            Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Retire),
            retire,
        ),
    ]
}

pub async fn get_fit(
    state: web::Data<AppState>,
    id: web::Path<ResearchJobId>,
) -> Result<WebResponse<ResearchJobView>, WebError> {
    let job = state
        .research_jobs
        .get(&id)
        .await?
        .filter(|job| job.kind == ResearchJobKind::TradePolicyFit)
        .ok_or_else(|| WebError::NotFound(format!("trade-policy fit not found: {id}")))?;
    Ok(WebResponse::ok(job))
}

pub async fn audits(
    state: web::Data<AppState>,
    id: web::Path<TradePolicyArtifactId>,
    query: web::Query<TradePolicyAuditListQuery>,
) -> Result<WebResponse<Paginated<TradePolicyGovernanceAuditView>>, WebError> {
    let page = state
        .trade_policies
        .page_audits(&id, query.into_inner())
        .await?
        .map(TradePolicyGovernanceAuditView::from);
    Ok(WebResponse::ok(page))
}

pub async fn list(
    state: web::Data<AppState>,
    query: web::Query<TradePolicyListQuery>,
) -> Result<WebResponse<Paginated<TradePolicySummaryView>>, WebError> {
    let page = state
        .trade_policies
        .page(query.into_inner())
        .await?
        .map(TradePolicySummaryView::from);
    Ok(WebResponse::ok(page))
}

pub async fn get(
    state: web::Data<AppState>,
    id: web::Path<TradePolicyArtifactId>,
) -> Result<WebResponse<TradePolicyDetailView>, WebError> {
    let info = state
        .trade_policies
        .find(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("trade policy not found: {id}")))?;
    Ok(WebResponse::ok(TradePolicyDetailView::from(info)))
}

pub async fn preflight(
    state: web::Data<AppState>,
    body: ValidatedJson<TradePolicyFitPreflightRequest>,
) -> Result<WebResponse<TradePolicyFitPreflightView>, WebError> {
    let request = body.into_inner();
    Ok(WebResponse::ok(
        state.trade_policies.preflight(&request).await?,
    ))
}

pub async fn fit(
    state: web::Data<AppState>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<FitTradePolicyRequest>,
) -> Result<WebResponse<ResearchJobView>, WebError> {
    let request = body.into_inner();
    let preflight = state
        .trade_policies
        .preflight(&TradePolicyFitPreflightRequest {
            selection: request.selection.clone(),
            activation_target: request.activation_target,
            candidates: request.candidates.clone(),
        })
        .await?;
    if preflight.publishable_input != TradePolicyPreflightCheckStatus::Pass {
        return Err(WebError::BadRequest(format!(
            "trade-policy fit preflight blocked enqueue: {}",
            preflight.messages.join("; ")
        )));
    }
    let reason = request.reason.clone();
    let job = state
        .research_jobs
        .enqueue_trade_policy_fit(
            request,
            JobSubmitContext {
                acting_role: acting_role.0.clone(),
                requested_by: None,
            },
        )
        .await?;
    op_ctx.set_action(OperationCategory::Governance, "trade_policy.fit");
    op_ctx.set_resource(ResourceType::Materialization, job.job_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "job_id": job.job_id.to_string(),
        "kind": "trade_policy_fit",
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }));
    Ok(WebResponse::accepted(job))
}

pub async fn validate(
    state: web::Data<AppState>,
    id: web::Path<TradePolicyArtifactId>,
    actor: AuthedActor,
    role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<TradePolicyGovernanceRequest>,
) -> Result<WebResponse<TradePolicyDetailView>, WebError> {
    transition(
        TransitionContext {
            state,
            artifact_id: id.into_inner(),
            actor,
            role,
            request_id,
            op_ctx,
            request: body.into_inner(),
        },
        TradePolicyStatus::Validated,
    )
    .await
}

pub async fn publish(
    state: web::Data<AppState>,
    id: web::Path<TradePolicyArtifactId>,
    actor: AuthedActor,
    role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<TradePolicyGovernanceRequest>,
) -> Result<WebResponse<TradePolicyDetailView>, WebError> {
    transition(
        TransitionContext {
            state,
            artifact_id: id.into_inner(),
            actor,
            role,
            request_id,
            op_ctx,
            request: body.into_inner(),
        },
        TradePolicyStatus::Published,
    )
    .await
}

pub async fn retire(
    state: web::Data<AppState>,
    id: web::Path<TradePolicyArtifactId>,
    actor: AuthedActor,
    role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<TradePolicyGovernanceRequest>,
) -> Result<WebResponse<TradePolicyDetailView>, WebError> {
    transition(
        TransitionContext {
            state,
            artifact_id: id.into_inner(),
            actor,
            role,
            request_id,
            op_ctx,
            request: body.into_inner(),
        },
        TradePolicyStatus::Retired,
    )
    .await
}

struct TransitionContext {
    state: web::Data<AppState>,
    artifact_id: TradePolicyArtifactId,
    actor: AuthedActor,
    role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    request: TradePolicyGovernanceRequest,
}

async fn transition(
    context: TransitionContext,
    target: TradePolicyStatus,
) -> Result<WebResponse<TradePolicyDetailView>, WebError> {
    let TransitionContext {
        state,
        artifact_id,
        actor,
        role,
        request_id,
        op_ctx,
        request,
    } = context;
    let actor_id = Uuid::parse_str(&actor.claims.sub)
        .map_err(|_| WebError::Unauthorized("invalid actor id".to_owned()))?;
    let info = state
        .trade_policies
        .transition(&artifact_id, target, actor_id, request.reason.clone())
        .await?;
    op_ctx.set_action(OperationCategory::Governance, "trade_policy.transition");
    op_ctx.set_resource(ResourceType::Materialization, artifact_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "artifact_id": artifact_id.to_string(),
        "target": target.as_str(),
        "acting_role": role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }));
    Ok(WebResponse::ok(TradePolicyDetailView::from(info)))
}
