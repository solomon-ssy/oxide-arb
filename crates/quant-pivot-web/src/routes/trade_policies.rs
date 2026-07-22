//! Governed trade-policy artifact endpoints.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::{
        api::{
            FitTradePolicyRequest, ResearchJobView, TradePolicyAuditListQuery,
            TradePolicyDetailView, TradePolicyEvidenceDownloadView,
            TradePolicyEvidenceRowListQuery, TradePolicyEvidenceRowView,
            TradePolicyFitPreflightRequest, TradePolicyFitPreflightView,
            TradePolicyGovernanceAuditView, TradePolicyGovernanceRequest, TradePolicyListQuery,
            TradePolicyPreflightCheckStatus, TradePolicySourceSliceObjectListQuery,
            TradePolicySourceSliceObjectView, TradePolicySourceSliceView, TradePolicySummaryView,
            TradePolicyTrialAttemptView, TradePolicyTrialListQuery, TradePolicyValidationJobParams,
            TradePolicyValidationListQuery, TradePolicyValidationRowListQuery,
            TradePolicyValidationRowView, TradePolicyValidationRunView,
        },
        pagination::Paginated,
        ports::JobSubmitContext,
    },
    enums::{
        operation_log::OperationCategory,
        quant::{ResearchJobKind, TradePolicyStatus},
        rbac::{Operation, ResourceType},
    },
    types::{
        ResearchJobId, ResearchProfileArtifact, ResearchProfileId, TradePolicyArtifactId,
        TradePolicyCohort, TradePolicyEvidenceObjectKind, TradePolicyValidationRunId, UserId,
    },
};

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
            "/research/trade-policy-profiles",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            profiles,
        ),
        spec(
            Method::GET,
            "/research/trade-policy-profiles/{id}/{version}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            profile,
        ),
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
            "/research/trade-policy-fits/{id}/trials",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            get_fit_trials,
        ),
        spec(
            Method::GET,
            "/research/trade-policies/{id}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            get,
        ),
        spec(
            Method::GET,
            "/research/trade-policies/{id}/cohorts",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            cohorts,
        ),
        spec(
            Method::GET,
            "/research/trade-policies/{id}/source-slice",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            source_slice,
        ),
        spec(
            Method::GET,
            "/research/trade-policies/{id}/source-slice/objects",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            source_slice_objects,
        ),
        spec(
            Method::GET,
            "/research/trade-policies/{id}/evidence/{kind}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            evidence_rows,
        ),
        spec(
            Method::GET,
            "/research/trade-policies/{id}/evidence/{kind}/download",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            evidence_download,
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
            Method::GET,
            "/research/trade-policies/{id}/validations",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            validations,
        ),
        spec(
            Method::GET,
            "/research/trade-policy-validations/{id}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            validation,
        ),
        spec(
            Method::GET,
            "/research/trade-policy-validations/{id}/rows",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            validation_rows,
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

pub async fn profiles(
    state: Data<AppState>,
) -> Result<WebResponse<Vec<ResearchProfileArtifact>>, WebError> {
    Ok(WebResponse::ok(state.trade_policies.list_profiles()?))
}

pub async fn profile(
    state: Data<AppState>,
    path: Path<(String, u32)>,
) -> Result<WebResponse<ResearchProfileArtifact>, WebError> {
    let (id, version) = path.into_inner();
    let profile = state
        .trade_policies
        .find_profile(&ResearchProfileId::new(&id), version)?
        .ok_or_else(|| WebError::NotFound(format!("research profile not found: {id}@{version}")))?;
    Ok(WebResponse::ok(profile))
}

pub async fn get_fit(
    state: Data<AppState>,
    id: Path<ResearchJobId>,
) -> Result<WebResponse<ResearchJobView>, WebError> {
    let job = state
        .research_jobs
        .get(&id)
        .await?
        .filter(|job| job.kind == ResearchJobKind::TradePolicyFit)
        .ok_or_else(|| WebError::NotFound(format!("trade-policy fit not found: {id}")))?;
    Ok(WebResponse::ok(job))
}

pub async fn get_fit_trials(
    state: Data<AppState>,
    id: Path<ResearchJobId>,
    query: Query<TradePolicyTrialListQuery>,
) -> Result<WebResponse<Paginated<TradePolicyTrialAttemptView>>, WebError> {
    let fit_job_id = id.into_inner();
    state
        .research_jobs
        .get(&fit_job_id)
        .await?
        .filter(|job| job.kind == ResearchJobKind::TradePolicyFit)
        .ok_or_else(|| WebError::NotFound(format!("trade-policy fit not found: {fit_job_id}")))?;
    let page = state
        .trade_policies
        .page_trials(&fit_job_id, query.into_inner())
        .await?
        .map(TradePolicyTrialAttemptView::from);
    Ok(WebResponse::ok(page))
}

pub async fn audits(
    state: Data<AppState>,
    id: Path<TradePolicyArtifactId>,
    query: Query<TradePolicyAuditListQuery>,
) -> Result<WebResponse<Paginated<TradePolicyGovernanceAuditView>>, WebError> {
    let page = state
        .trade_policies
        .page_audits(&id, query.into_inner())
        .await?
        .map(TradePolicyGovernanceAuditView::from);
    Ok(WebResponse::ok(page))
}

pub async fn list(
    state: Data<AppState>,
    query: Query<TradePolicyListQuery>,
) -> Result<WebResponse<Paginated<TradePolicySummaryView>>, WebError> {
    let page = state
        .trade_policies
        .page(query.into_inner())
        .await?
        .map(TradePolicySummaryView::from);
    Ok(WebResponse::ok(page))
}

pub async fn get(
    state: Data<AppState>,
    id: Path<TradePolicyArtifactId>,
) -> Result<WebResponse<TradePolicyDetailView>, WebError> {
    let info = state
        .trade_policies
        .find(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("trade policy not found: {id}")))?;
    Ok(WebResponse::ok(TradePolicyDetailView::from(info)))
}

pub async fn cohorts(
    state: Data<AppState>,
    id: Path<TradePolicyArtifactId>,
) -> Result<WebResponse<Vec<TradePolicyCohort>>, WebError> {
    let info = state
        .trade_policies
        .find(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("trade policy not found: {id}")))?;
    Ok(WebResponse::ok(info.payload_json.cohorts))
}

pub async fn source_slice(
    state: Data<AppState>,
    id: Path<TradePolicyArtifactId>,
) -> Result<WebResponse<TradePolicySourceSliceView>, WebError> {
    let view = state
        .trade_policies
        .source_slice(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("trade policy not found: {id}")))?;
    Ok(WebResponse::ok(view))
}

pub async fn source_slice_objects(
    state: Data<AppState>,
    id: Path<TradePolicyArtifactId>,
    query: Query<TradePolicySourceSliceObjectListQuery>,
) -> Result<WebResponse<Paginated<TradePolicySourceSliceObjectView>>, WebError> {
    let page = state
        .trade_policies
        .page_source_slice_objects(&id, query.into_inner())
        .await?
        .ok_or_else(|| WebError::NotFound(format!("trade policy not found: {id}")))?;
    Ok(WebResponse::ok(page))
}

pub async fn evidence_download(
    state: Data<AppState>,
    path: Path<(TradePolicyArtifactId, TradePolicyEvidenceObjectKind)>,
) -> Result<WebResponse<TradePolicyEvidenceDownloadView>, WebError> {
    let (artifact_id, kind) = path.into_inner();
    let view = state
        .trade_policies
        .evidence_download(&artifact_id, kind)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("trade policy not found: {artifact_id}")))?;
    Ok(WebResponse::ok(view))
}

pub async fn evidence_rows(
    state: Data<AppState>,
    path: Path<(TradePolicyArtifactId, TradePolicyEvidenceObjectKind)>,
    query: Query<TradePolicyEvidenceRowListQuery>,
) -> Result<WebResponse<Paginated<TradePolicyEvidenceRowView>>, WebError> {
    let (artifact_id, kind) = path.into_inner();
    let page = state
        .trade_policies
        .page_evidence_rows(&artifact_id, kind, query.into_inner())
        .await?
        .ok_or_else(|| WebError::NotFound(format!("trade policy not found: {artifact_id}")))?;
    Ok(WebResponse::ok(page))
}

pub async fn preflight(
    state: Data<AppState>,
    body: ValidatedJson<TradePolicyFitPreflightRequest>,
) -> Result<WebResponse<TradePolicyFitPreflightView>, WebError> {
    let request = body.into_inner();
    Ok(WebResponse::ok(
        state.trade_policies.preflight(&request).await?,
    ))
}

pub async fn fit(
    state: Data<AppState>,
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
            evaluation_track: request.evaluation_track,
            candidates: request.candidates.clone(),
        })
        .await?;
    if preflight.publishable_input != TradePolicyPreflightCheckStatus::Pass {
        return Err(WebError::BadRequest(format!(
            "trade-policy fit preflight blocked enqueue: {:?}",
            preflight
                .blockers
                .iter()
                .map(|blocker| &blocker.detail)
                .collect::<Vec<_>>()
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
    }))?;
    Ok(WebResponse::accepted(job))
}

pub async fn validate(
    state: Data<AppState>,
    id: Path<TradePolicyArtifactId>,
    actor: AuthedActor,
    role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<TradePolicyGovernanceRequest>,
) -> Result<WebResponse<ResearchJobView>, WebError> {
    let artifact_id = id.into_inner();
    let actor_id = actor
        .claims
        .sub
        .parse::<UserId>()
        .map_err(|_| WebError::Unauthorized("invalid actor id".to_owned()))?;
    let request = body.into_inner();
    let reason = request.reason.clone();
    let job = state
        .research_jobs
        .enqueue_trade_policy_validation(
            TradePolicyValidationJobParams {
                validation_run_id: TradePolicyValidationRunId::from_v7(),
                artifact_id,
                actor_id,
                reason: request.reason,
            },
            JobSubmitContext {
                acting_role: role.0.clone(),
                requested_by: Some(actor.claims.sub),
            },
        )
        .await?;
    op_ctx.set_action(OperationCategory::Governance, "trade_policy.validate");
    op_ctx.set_resource(ResourceType::Materialization, artifact_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "artifact_id": artifact_id.to_string(),
        "job_id": job.job_id.to_string(),
        "acting_role": role.0,
        "request_id": request_id.0,
        "reason": reason,
    }))?;
    Ok(WebResponse::accepted(job))
}

pub async fn validations(
    state: Data<AppState>,
    id: Path<TradePolicyArtifactId>,
    query: Query<TradePolicyValidationListQuery>,
) -> Result<WebResponse<Paginated<TradePolicyValidationRunView>>, WebError> {
    let page = state
        .trade_policies
        .page_validations(&id, query.into_inner())
        .await?
        .map(TradePolicyValidationRunView::from);
    Ok(WebResponse::ok(page))
}

pub async fn validation(
    state: Data<AppState>,
    id: Path<TradePolicyValidationRunId>,
) -> Result<WebResponse<TradePolicyValidationRunView>, WebError> {
    let run = state
        .trade_policies
        .find_validation(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("trade-policy validation not found: {id}")))?;
    Ok(WebResponse::ok(TradePolicyValidationRunView::from(run)))
}

pub async fn validation_rows(
    state: Data<AppState>,
    id: Path<TradePolicyValidationRunId>,
    query: Query<TradePolicyValidationRowListQuery>,
) -> Result<WebResponse<Paginated<TradePolicyValidationRowView>>, WebError> {
    if state.trade_policies.find_validation(&id).await?.is_none() {
        return Err(WebError::NotFound(format!(
            "trade-policy validation not found: {id}"
        )));
    }
    let page = state
        .trade_policies
        .page_validation_rows(&id, query.into_inner())
        .await?
        .map(TradePolicyValidationRowView::from);
    Ok(WebResponse::ok(page))
}

pub async fn publish(
    state: Data<AppState>,
    id: Path<TradePolicyArtifactId>,
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
    state: Data<AppState>,
    id: Path<TradePolicyArtifactId>,
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
    state: Data<AppState>,
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
    let actor_id = actor
        .claims
        .sub
        .parse::<UserId>()
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
    }))?;
    Ok(WebResponse::ok(TradePolicyDetailView::from(info)))
}
