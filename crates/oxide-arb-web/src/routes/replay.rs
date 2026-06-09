//! Replay (materialization run) read endpoints (`Replay:Read`).
//!
//! Exposes the status of a materialization/replay run and its per-stage report
//! history. Enqueue (`POST /replay`, `Replay:Create`, governed) is wired via
//! the replay port in [`crate::routes::replay::enqueue`].

use actix_web::{http::Method, web};
use oxide_arb_models::{
    domain::{
        ControlFactorMaterializationRunView, ControlFactorStageReportView, ReplayCreateRequest,
        ReplayEnqueueView,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    types::MaterializationRunId,
};

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Replay routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::POST,
            "/replay",
            Rule::ActingRoleGoverned(ResourceType::Replay, Operation::Create),
            enqueue,
        ),
        spec(
            Method::GET,
            "/replay/{run_id}",
            Rule::ResourceOp(ResourceType::Replay, Operation::Read),
            status,
        ),
        spec(
            Method::GET,
            "/replay/{run_id}/history",
            Rule::ResourceOp(ResourceType::Replay, Operation::Read),
            history,
        ),
    ]
}

/// `POST /api/replay` — enqueue a backfill/replay materialization run.
pub async fn enqueue(
    state: web::Data<AppState>,
    _acting_role: ActingRole,
    op_ctx: OperationCtx,
    body: ValidatedJson<ReplayCreateRequest>,
) -> Result<WebResponse<ReplayEnqueueView>, WebError> {
    let body = body.into_inner();
    op_ctx.set_action(OperationCategory::Replay, "replay.enqueue");
    op_ctx.set_detail(serde_json::json!({
        "from": body.from,
        "to": body.to,
        "reason": body.reason,
        "force_new_run": body.force_new_run,
    }));
    let result = state.replay.enqueue(body.into()).await?;
    Ok(WebResponse::ok(ReplayEnqueueView {
        created: result.created,
        run: result.run.into(),
    }))
}

/// `GET /api/replay/{run_id}` — current status of a materialization/replay run.
pub async fn status(
    state: web::Data<AppState>,
    run_id: web::Path<MaterializationRunId>,
) -> Result<WebResponse<ControlFactorMaterializationRunView>, WebError> {
    let run_id = run_id.into_inner();
    let run = state
        .control_factors
        .load_materialization_run(&run_id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("materialization run not found: {run_id}")))?;
    Ok(WebResponse::ok(run.into()))
}

/// `GET /api/replay/{run_id}/history` — per-stage report history for a run.
pub async fn history(
    state: web::Data<AppState>,
    run_id: web::Path<MaterializationRunId>,
) -> Result<WebResponse<Vec<ControlFactorStageReportView>>, WebError> {
    let reports = state
        .control_factors
        .list_stage_reports(&run_id.into_inner())
        .await?;
    Ok(WebResponse::ok(
        reports.into_iter().map(Into::into).collect(),
    ))
}
