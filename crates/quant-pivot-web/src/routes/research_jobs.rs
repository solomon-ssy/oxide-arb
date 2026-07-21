//! Durable research-job task-center endpoints.
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/research/jobs` | `materialization:read` | Page the job ledger |
//! | GET | `/research/jobs/{id}` | `materialization:read` | Poll one job |
//! | POST | `/research/jobs/{id}/cancel` | governed `materialization:create` | Cancel (terminal if queued, cooperative if running) |
//! | POST | `/research/jobs/{id}/retry` | governed `materialization:create` | Re-enqueue a terminal job's frozen params |

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::{
        api::{
            CancelResearchJobRequest, ResearchJobListQuery, ResearchJobView,
            RetryResearchJobRequest,
        },
        pagination::Paginated,
        ports::JobSubmitContext,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    types::ResearchJobId,
};

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, RequestId, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Research-job task-center routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/research/jobs",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list,
        ),
        // Literal segments before `{id}` so `cancel` / `retry` are not captured.
        spec(
            Method::POST,
            "/research/jobs/{id}/cancel",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            cancel,
        ),
        spec(
            Method::POST,
            "/research/jobs/{id}/retry",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            retry,
        ),
        spec(
            Method::GET,
            "/research/jobs/{id}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            get_by_id,
        ),
    ]
}

/// `GET /api/research/jobs` — paginated job ledger for the task center.
pub async fn list(
    state: Data<AppState>,
    query: Query<ResearchJobListQuery>,
) -> Result<WebResponse<Paginated<ResearchJobView>>, WebError> {
    let page = state.research_jobs.list(query.into_inner()).await?;
    Ok(WebResponse::ok(page))
}

/// `GET /api/research/jobs/{id}` — single job (UI poll target).
pub async fn get_by_id(
    state: Data<AppState>,
    id: Path<ResearchJobId>,
) -> Result<WebResponse<ResearchJobView>, WebError> {
    let view = state
        .research_jobs
        .get(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("research job not found: {id}")))?;
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/jobs/{id}/cancel` — cancel a job.
pub async fn cancel(
    state: Data<AppState>,
    id: Path<ResearchJobId>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<CancelResearchJobRequest>,
) -> Result<WebResponse<ResearchJobView>, WebError> {
    let job_id = id.into_inner();
    let request = body.into_inner();
    let reason = request.reason.clone();
    let view = state
        .research_jobs
        .cancel(
            &job_id,
            request.reason,
            JobSubmitContext {
                acting_role: acting_role.0.clone(),
                requested_by: None,
            },
        )
        .await?;
    op_ctx.set_action(OperationCategory::Other, "research.job.cancel");
    op_ctx.set_resource(ResourceType::Materialization, job_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "job_id": job_id.to_string(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }))?;
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/jobs/{id}/retry` — re-enqueue a terminal job.
pub async fn retry(
    state: Data<AppState>,
    id: Path<ResearchJobId>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RetryResearchJobRequest>,
) -> Result<WebResponse<ResearchJobView>, WebError> {
    let job_id = id.into_inner();
    let request = body.into_inner();
    let reason = request.reason.clone();
    let view = state
        .research_jobs
        .retry(
            &job_id,
            request.reason,
            JobSubmitContext {
                acting_role: acting_role.0.clone(),
                requested_by: None,
            },
        )
        .await?;
    op_ctx.set_action(OperationCategory::Other, "research.job.retry");
    op_ctx.set_resource(ResourceType::Materialization, view.job_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "job_id": view.job_id.to_string(),
        "parent_job_id": job_id.to_string(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }))?;
    Ok(WebResponse::accepted(view))
}
