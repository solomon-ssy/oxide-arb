//! Unified calibration-artifact admin endpoints.
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/research/calibration-artifacts` | `materialization:read` | Paginated artifact catalog |
//! | GET | `/research/calibration-artifacts/{id}` | `materialization:read` | Full detail |
//! | POST | `/research/calibration-artifacts/fit-bias-table` | `materialization:create` | Enqueue bias-table fit |
//! | POST | `/research/calibration-artifacts/fit-model-calibrator` | `materialization:create` | Enqueue calibrator fit |
//! | POST | `/research/calibration-artifacts/{id}/activate` | `materialization:create` | Activate reviewed artifact |

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::{
        api::{
            ActivateCalibrationArtifactRequest, CalibrationArtifactDetailView,
            CalibrationArtifactListQuery, CalibrationArtifactSummaryView, FitBiasTableRequest,
            FitModelCalibratorRequest, ResearchJobView,
        },
        pagination::Paginated,
        ports::JobSubmitContext,
    },
    enums::{
        operation_log::OperationCategory,
        quant::CalibrationKind,
        rbac::{Operation, ResourceType},
    },
    types::CalibrationArtifactId,
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
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            activate,
        ),
    ]
}

/// `GET /api/research/calibration-artifacts` — paginated artifact catalog.
pub async fn list(
    state: Data<AppState>,
    query: Query<CalibrationArtifactListQuery>,
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
    state: Data<AppState>,
    id: Path<CalibrationArtifactId>,
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
    state: Data<AppState>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<FitBiasTableRequest>,
) -> Result<WebResponse<ResearchJobView>, WebError> {
    let request = body.into_inner();
    let reason = request.reason.clone();
    let decision_policy_snapshot_id = state
        .runtime_config
        .load_current()
        .await?
        .ok_or_else(|| {
            WebError::Conflict("no active runtime-config version to fit against".to_owned())
        })?
        .decision_policy_snapshot_id;
    let job = state
        .research_jobs
        .enqueue_bias_table_fit(
            request,
            decision_policy_snapshot_id,
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
    }))?;
    Ok(WebResponse::accepted(job))
}

/// `POST /api/research/calibration-artifacts/fit-model-calibrator` — enqueue calibrator fit.
pub async fn fit_model_calibrator(
    state: Data<AppState>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<FitModelCalibratorRequest>,
) -> Result<WebResponse<ResearchJobView>, WebError> {
    let request = body.into_inner();
    let reason = request.reason.clone();
    let decision_policy_snapshot_id = state
        .runtime_config
        .load_current()
        .await?
        .ok_or_else(|| {
            WebError::Conflict("no active runtime-config version to fit against".to_owned())
        })?
        .decision_policy_snapshot_id;
    let job = state
        .research_jobs
        .enqueue_model_calibration_fit(
            request,
            decision_policy_snapshot_id,
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
    }))?;
    Ok(WebResponse::accepted(job))
}

/// `POST /api/research/calibration-artifacts/{id}/activate` — pin a `market_price_bias` ref.
pub async fn activate(
    state: Data<AppState>,
    id: Path<CalibrationArtifactId>,
    _actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<ActivateCalibrationArtifactRequest>,
) -> Result<WebResponse<CalibrationArtifactDetailView>, WebError> {
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
    // Artifact activation is its own governed ledger transition. It never
    // creates or activates a Config revision; the operator must explicitly
    // select the artifact in the ModelRouting workflow afterwards.
    state
        .calibration_artifacts
        .mark_active(&artifact_id)
        .await?;
    let activated = state
        .calibration_artifacts
        .find(&artifact_id)
        .await?
        .ok_or_else(|| {
            WebError::Internal(format!(
                "activated calibration artifact disappeared: {artifact_id}"
            ))
        })?;
    op_ctx.set_action(OperationCategory::Other, "calibration_artifact.activate");
    op_ctx.set_resource(ResourceType::Materialization, artifact_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "artifact_id": artifact_id.to_string(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }))?;
    Ok(WebResponse::ok(CalibrationArtifactDetailView::from(
        activated,
    )))
}
