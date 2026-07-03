//! Offline training-dataset admin endpoints (Phase 3.5).
//!
//! # UI integration contract
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | GET | `/research/training-datasets/{id}` | `materialization:read` | Poll ledger status after build |
//! | POST | `/research/training-datasets/plan` | `materialization:create` | Dry-run sample grid (fast) |
//! | POST | `/research/training-datasets/build` | `materialization:create` | Plan + materialize Parquet + ledger |
//!
//! Recommended SPA flow:
//!
//! 1. Operator selects `runtime_config_version_id` + `model_spec_id` + window.
//! 2. Call **plan** → display `planned_samples` and save `training_dataset_id`.
//! 3. Call **build** with the same body **plus** the plan's `training_dataset_id`.
//! 4. Poll **GET** every few seconds until `status` is terminal.
//!
//! # Sync vs async
//!
//! Build currently runs synchronously on the HTTP worker (may take minutes for
//! large windows). Phase 03.7 will introduce an async job queue + WS channel
//! `materialization.run_update` for progress; until then the UI should disable
//! double-submit and show a long-running spinner.

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        BuildTrainingDatasetRequest, MaterializationRunKind, Paginated, TrainingDatasetListQuery,
        TrainingDatasetPlanView, TrainingDatasetView,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    types::TrainingDatasetId,
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

/// Training-dataset research routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/research/training-datasets",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list,
        ),
        // Literal segments before `{id}` so `plan` / `build` are not captured as IDs.
        spec(
            Method::POST,
            "/research/training-datasets/plan",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            plan,
        ),
        spec(
            Method::POST,
            "/research/training-datasets/build",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            build,
        ),
        spec(
            Method::GET,
            "/research/training-datasets/{id}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            get_by_id,
        ),
    ]
}

/// `GET /api/research/training-datasets` — paginated ledger catalog.
pub async fn list(
    state: web::Data<AppState>,
    query: web::Query<TrainingDatasetListQuery>,
) -> Result<WebResponse<Paginated<TrainingDatasetView>>, WebError> {
    let page = state
        .research_catalog
        .list_training_datasets(query.into_inner())
        .await?
        .map(TrainingDatasetView::from);
    Ok(WebResponse::ok(page))
}

/// `GET /api/research/training-datasets/{id}` — ledger row for UI polling.
pub async fn get_by_id(
    state: web::Data<AppState>,
    id: web::Path<TrainingDatasetId>,
) -> Result<WebResponse<TrainingDatasetView>, WebError> {
    let info = state
        .training_datasets
        .find_by_id(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("training_dataset not found: {id}")))?;
    Ok(WebResponse::ok(TrainingDatasetView::from(info)))
}

/// `POST /api/research/training-datasets/plan` — dry plan; no artifact or ledger write.
pub async fn plan(
    state: web::Data<AppState>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<BuildTrainingDatasetRequest>,
) -> Result<WebResponse<TrainingDatasetPlanView>, WebError> {
    let request = body.into_inner();
    let view = state.training_datasets.plan(request.clone()).await?;
    op_ctx.set_action(OperationCategory::Other, "research.training_dataset.plan");
    op_ctx.set_resource(
        ResourceType::Materialization,
        view.training_dataset_id.to_string(),
    );
    op_ctx.set_detail(serde_json::json!({
        "planned_samples": view.planned_samples,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }));
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/training-datasets/build` — full offline materialization.
pub async fn build(
    state: web::Data<AppState>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<BuildTrainingDatasetRequest>,
) -> Result<WebResponse<TrainingDatasetView>, WebError> {
    let request = body.into_inner();
    let reason = request.reason.clone();
    let view = state.training_datasets.build(request).await?;
    // Fan out a `materialization.run_update` revision hint so other operators'
    // open workbench catalogs re-fetch (the build itself is synchronous here).
    state.publish_materialization_run(
        view.training_dataset_id.to_string(),
        MaterializationRunKind::Dataset,
        view.status.into(),
    );
    op_ctx.set_action(OperationCategory::Other, "research.training_dataset.build");
    op_ctx.set_resource(
        ResourceType::Materialization,
        view.training_dataset_id.to_string(),
    );
    op_ctx.set_detail(serde_json::json!({
        "status": view.status.as_str(),
        "sample_count": view.sample_count,
        "dataset_hash": view.dataset_hash,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }));
    Ok(WebResponse::ok(view))
}
