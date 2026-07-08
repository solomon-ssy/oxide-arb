//! Unified calibration-artifact admin endpoints (Phase 11.3 §10).
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/research/calibration-artifacts` | `materialization:read` | Paginated artifact catalog |
//! | GET | `/research/calibration-artifacts/{id}` | `materialization:read` | Full detail |
//! | POST | `/research/calibration-artifacts/fit-bias-table` | `materialization:create` | Enqueue bias-table fit |
//! | POST | `/research/calibration-artifacts/fit-model-calibrator` | `materialization:create` | Enqueue calibrator fit |
//! | POST | `/research/calibration-artifacts/{id}/activate` | `runtime_config:create` | Pin bias table ref |

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        ActivateCalibrationArtifactRequest, CalibrationArtifactDetailView,
        CalibrationArtifactListQuery, CalibrationArtifactSummaryView, FitBiasTableRequest,
        FitModelCalibratorRequest, JobSubmitContext, NewRuntimeConfigVersion, Paginated,
        ResearchJobView, RuntimeConfigVersionView,
    },
    enums::{
        operation_log::OperationCategory,
        quant::CalibrationKind,
        rbac::{Operation, ResourceType},
        runtime_config::RuntimeConfigVersionSource,
    },
    hashing::CanonicalDigest,
    runtime_config::mask_config_json,
    types::{CalibrationArtifactId, RuntimeConfigVersionId},
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

/// Unified calibration-artifact routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/research/calibration-artifacts",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list,
        ),
        spec(
            Method::POST,
            "/research/calibration-artifacts/fit-bias-table",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            fit_bias_table,
        ),
        spec(
            Method::POST,
            "/research/calibration-artifacts/fit-model-calibrator",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            fit_model_calibrator,
        ),
        spec(
            Method::GET,
            "/research/calibration-artifacts/{id}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            get,
        ),
        spec(
            Method::POST,
            "/research/calibration-artifacts/{id}/activate",
            Rule::ActingRoleGoverned(ResourceType::RuntimeConfig, Operation::Create),
            activate,
        ),
    ]
}

/// `GET /api/research/calibration-artifacts` — paginated artifact catalog.
pub async fn list(
    state: web::Data<AppState>,
    query: web::Query<CalibrationArtifactListQuery>,
) -> Result<WebResponse<Paginated<CalibrationArtifactSummaryView>>, WebError> {
    let page = state
        .calibration_artifacts
        .page(query.into_inner())
        .await?
        .map(CalibrationArtifactSummaryView::from);
    Ok(WebResponse::ok(page))
}

/// `GET /api/research/calibration-artifacts/{id}` — full artifact detail.
pub async fn get(
    state: web::Data<AppState>,
    id: web::Path<CalibrationArtifactId>,
) -> Result<WebResponse<CalibrationArtifactDetailView>, WebError> {
    let info = state
        .calibration_artifacts
        .find(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("calibration artifact not found: {id}")))?;
    Ok(WebResponse::ok(CalibrationArtifactDetailView::from(info)))
}

/// `POST /api/research/calibration-artifacts/fit-bias-table` — enqueue bias-table fit.
pub async fn fit_bias_table(
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
    op_ctx.set_action(
        OperationCategory::Other,
        "calibration_artifact.fit_bias_table",
    );
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

/// `POST /api/research/calibration-artifacts/fit-model-calibrator` — enqueue calibrator fit.
pub async fn fit_model_calibrator(
    state: web::Data<AppState>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<FitModelCalibratorRequest>,
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
        .enqueue_model_calibration_fit(
            request,
            runtime_config_version_id,
            JobSubmitContext {
                acting_role: acting_role.0.clone(),
                requested_by: None,
            },
        )
        .await?;
    op_ctx.set_action(
        OperationCategory::Other,
        "calibration_artifact.fit_model_calibrator",
    );
    op_ctx.set_resource(ResourceType::Materialization, job.job_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "job_id": job.job_id.to_string(),
        "kind": "model_calibration_fit",
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }));
    Ok(WebResponse::accepted(job))
}

/// `POST /api/research/calibration-artifacts/{id}/activate` — pin a `market_price_bias` ref.
pub async fn activate(
    state: web::Data<AppState>,
    id: web::Path<CalibrationArtifactId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<ActivateCalibrationArtifactRequest>,
) -> Result<WebResponse<RuntimeConfigVersionView>, WebError> {
    let artifact_id = id.into_inner();
    let reason = body.into_inner().reason;
    let info = state
        .calibration_artifacts
        .find(&artifact_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!("calibration artifact not found: {artifact_id}"))
        })?;
    if info.kind != CalibrationKind::MarketPriceBias {
        return Err(WebError::Conflict(format!(
            "artifact {artifact_id} is {:?}, not market_price_bias — cannot activate as bias table ref",
            info.kind
        )));
    }
    // Mark this bias table active (deactivating any previously-active one)
    // before staging the runtime-config version that points at it — the
    // operator-facing "activate" intent must be recorded on the artifact
    // ledger itself, not only implied by a config field (Phase 11.3 §3.4
    // `active` governance).
    state
        .calibration_artifacts
        .mark_active(&artifact_id)
        .await?;
    let mut config = state.runtime_config_apply.current().as_ref().clone();
    config.factors.structural.favorite_longshot.bias_table_ref =
        Some(artifact_id.as_uuid().to_string());

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

    op_ctx.set_action(
        OperationCategory::RuntimeConfig,
        "calibration_artifact.activate",
    );
    op_ctx.set_resource(
        ResourceType::RuntimeConfig,
        version.runtime_config_version_id.to_string(),
    );
    op_ctx.set_state_hashes(None, Some(config_hash.as_str().to_owned()));
    op_ctx.set_detail(serde_json::json!({
        "artifact_id": artifact_id.to_string(),
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
