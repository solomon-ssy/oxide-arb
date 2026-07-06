//! Favorite-longshot bias-table admin endpoints (Phase 11.2.1).
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/research/bias-tables` | `materialization:read` | Paginated artifact catalog |
//! | GET | `/research/bias-tables/{id}` | `materialization:read` | Full curve detail |
//! | POST | `/research/bias-tables/fit` | `materialization:create` | Enqueue an async fit job |
//! | POST | `/research/bias-tables/{id}/activate` | `runtime_config:create` | Stage a config version pinning the ref |
//!
//! The fit runs on the `ResearchJobWorker` (fail-closed: a thin greenfield
//! spine mints no artifact). Activation stages an immutable runtime-config
//! version whose `factors.structural.favorite_longshot.bias_table_ref` points at
//! the table; the operator promotes it through the runtime-config governance flow.

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        ActivateBiasTableRequest, BiasTableDetailView, BiasTableListQuery, BiasTableSummaryView,
        FitBiasTableRequest, JobSubmitContext, NewRuntimeConfigVersion, Paginated, ResearchJobView,
        RuntimeConfigVersionView,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
        runtime_config::RuntimeConfigVersionSource,
    },
    hashing::CanonicalDigest,
    runtime_config::mask_config_json,
    types::{FavoriteLongshotBiasTableId, RuntimeConfigVersionId},
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

/// Favorite-longshot bias-table routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/research/bias-tables",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list,
        ),
        // Literal segment before `{id}` so `fit` is not captured as an id.
        spec(
            Method::POST,
            "/research/bias-tables/fit",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            fit,
        ),
        spec(
            Method::GET,
            "/research/bias-tables/{id}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            get,
        ),
        spec(
            Method::POST,
            "/research/bias-tables/{id}/activate",
            Rule::ActingRoleGoverned(ResourceType::RuntimeConfig, Operation::Create),
            activate,
        ),
    ]
}

/// `GET /api/research/bias-tables` — paginated bias-table ledger catalog.
pub async fn list(
    state: web::Data<AppState>,
    query: web::Query<BiasTableListQuery>,
) -> Result<WebResponse<Paginated<BiasTableSummaryView>>, WebError> {
    let page = state
        .favorite_longshot
        .page(query.into_inner())
        .await?
        .map(BiasTableSummaryView::from);
    Ok(WebResponse::ok(page))
}

/// `GET /api/research/bias-tables/{id}` — full per-category curve detail.
pub async fn get(
    state: web::Data<AppState>,
    id: web::Path<FavoriteLongshotBiasTableId>,
) -> Result<WebResponse<BiasTableDetailView>, WebError> {
    let info = state
        .favorite_longshot
        .find(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("bias table not found: {id}")))?;
    Ok(WebResponse::ok(BiasTableDetailView::from(info)))
}

/// `POST /api/research/bias-tables/fit` — enqueue an async bias-table fit.
///
/// Returns `202 Accepted` with the queued [`ResearchJobView`]; the fit runs on
/// the `ResearchJobWorker` against the active runtime-config version (frozen
/// here). Poll the job / listen on `materialization.run_update`.
pub async fn fit(
    state: web::Data<AppState>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<FitBiasTableRequest>,
) -> Result<WebResponse<ResearchJobView>, WebError> {
    let request = body.into_inner();
    let reason = request.reason.clone();
    let runtime_config_version_id = state
        .runtime_config
        .load_current()
        .await?
        .ok_or_else(|| {
            WebError::Conflict("no active runtime-config version to fit against".to_owned())
        })?
        .runtime_config_version_id;
    let job = state
        .research_jobs
        .enqueue_bias_table_fit(
            request,
            runtime_config_version_id,
            JobSubmitContext {
                acting_role: acting_role.0.clone(),
                requested_by: None,
            },
        )
        .await?;
    op_ctx.set_action(OperationCategory::Other, "bias_table.fit");
    op_ctx.set_resource(ResourceType::Materialization, job.job_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "job_id": job.job_id.to_string(),
        "kind": "bias_table_fit",
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }));
    Ok(WebResponse::accepted(job))
}

/// `POST /api/research/bias-tables/{id}/activate` — stage a runtime-config
/// version pinning this table as the favorite-longshot bias-table ref.
///
/// This does not itself flip the live config: it mints an immutable version
/// (governed) that the operator activates through the runtime-config flow, so
/// activation stays a single audited money-state-preflighted transition.
pub async fn activate(
    state: web::Data<AppState>,
    id: web::Path<FavoriteLongshotBiasTableId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<ActivateBiasTableRequest>,
) -> Result<WebResponse<RuntimeConfigVersionView>, WebError> {
    let bias_table_id = id.into_inner();
    let reason = body.into_inner().reason;
    // The table must exist before it can be pinned.
    if state
        .favorite_longshot
        .find(&bias_table_id)
        .await?
        .is_none()
    {
        return Err(WebError::NotFound(format!(
            "bias table not found: {bias_table_id}"
        )));
    }
    let mut config = state.runtime_config_apply.current().as_ref().clone();
    config.factors.structural.favorite_longshot.bias_table_ref =
        Some(bias_table_id.as_uuid().to_string());

    let config_json = config.to_json();
    let config_hash = CanonicalDigest::content_hash_json(&config_json)
        .map_err(|error| WebError::Internal(error.to_string()))?;
    let version = NewRuntimeConfigVersion {
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        config_hash: config_hash.clone(),
        schema_version: config.schema_version,
        config_json,
        source: RuntimeConfigVersionSource::Operator,
        created_by: actor.claims.username.clone(),
        reason: reason.clone(),
    };
    let version = state.runtime_config.create_version(version).await?;

    op_ctx.set_action(OperationCategory::RuntimeConfig, "bias_table.activate");
    op_ctx.set_resource(
        ResourceType::RuntimeConfig,
        version.runtime_config_version_id.to_string(),
    );
    op_ctx.set_state_hashes(None, Some(config_hash.as_str().to_owned()));
    op_ctx.set_detail(serde_json::json!({
        "bias_table_id": bias_table_id.to_string(),
        "runtime_config_version_id": version.runtime_config_version_id.to_string(),
        "config_hash": config_hash,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }));
    let masked = mask_config_json(&version.config_json);
    Ok(WebResponse::ok(RuntimeConfigVersionView::from_info(
        version, masked,
    )))
}
