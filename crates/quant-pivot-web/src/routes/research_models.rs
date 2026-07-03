//! Offline trainer + backtest admin endpoints (Phase 3.6).
//!
//! # UI integration contract
//!
//! | Method | Path | Permission | Purpose |
//! |--------|------|------------|---------|
//! | POST | `/research/models/train` | `materialization:create` | Train + register a Candidate version |
//! | GET | `/research/models/{id}` | `materialization:read` | Poll a registered version |
//! | POST | `/research/models/{id}/backtest` | `replay:create` | PIT backtest over a frozen dataset |
//! | GET | `/research/backtest-reports/{id}` | `replay:read` | Fetch a stored backtest report |
//!
//! Recommended SPA flow:
//!
//! 1. Build a `ready` training dataset (03.5 Admin API).
//! 2. **train** → save the returned `model_version_id`.
//! 3. **backtest** with a (holdout) `training_dataset_id` + `calibrate` flag.
//! 4. Poll **GET** the version / report.
//!
//! Training + backtest run synchronously on the HTTP worker (may take seconds to
//! minutes); the SPA should disable double-submit and show a spinner. An async
//! job queue is a later concern (mirrors 03.5).

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        BacktestReportListQuery, BacktestReportView, ComparisonReportListQuery, CoreEvent,
        MaterializationRunEvent, MaterializationRunKind, MaterializationRunStatus,
        ModelComparisonReportView, ModelSpecListQuery, ModelVersionListQuery, Paginated,
        QuantModelSpecView, RunBacktestRequest, TrainModelRequest, TrainedModelView,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    types::{BacktestReportId, ModelComparisonReportId, ModelVersionId},
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

/// Trainer + backtest research routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/research/model-specs",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list_model_specs,
        ),
        spec(
            Method::GET,
            "/research/models",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list_models,
        ),
        spec(
            Method::POST,
            "/research/models/train",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            train,
        ),
        spec(
            Method::GET,
            "/research/models/{id}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            get_version,
        ),
        spec(
            Method::GET,
            "/research/backtest-reports",
            Rule::ResourceOp(ResourceType::Replay, Operation::Read),
            list_backtest_reports,
        ),
        spec(
            Method::GET,
            "/research/comparison-reports",
            Rule::ResourceOp(ResourceType::Replay, Operation::Read),
            list_comparison_reports,
        ),
        spec(
            Method::POST,
            "/research/models/{id}/backtest",
            Rule::ActingRoleGoverned(ResourceType::Replay, Operation::Create),
            backtest,
        ),
        spec(
            Method::GET,
            "/research/backtest-reports/{id}",
            Rule::ResourceOp(ResourceType::Replay, Operation::Read),
            get_backtest_report,
        ),
        spec(
            Method::GET,
            "/research/comparison-reports/{id}",
            Rule::ResourceOp(ResourceType::Replay, Operation::Read),
            get_comparison_report,
        ),
    ]
}

/// `GET /api/research/model-specs` — paginated model-spec catalog (the
/// dataset/training selector source).
pub async fn list_model_specs(
    state: web::Data<AppState>,
    query: web::Query<ModelSpecListQuery>,
) -> Result<WebResponse<Paginated<QuantModelSpecView>>, WebError> {
    let page = state
        .research_catalog
        .list_model_specs(query.into_inner())
        .await?
        .map(QuantModelSpecView::from);
    Ok(WebResponse::ok(page))
}

/// `GET /api/research/models` — paginated trained-model registry catalog.
pub async fn list_models(
    state: web::Data<AppState>,
    query: web::Query<ModelVersionListQuery>,
) -> Result<WebResponse<Paginated<TrainedModelView>>, WebError> {
    let page = state
        .research_catalog
        .list_models(query.into_inner())
        .await?
        .map(TrainedModelView::from);
    Ok(WebResponse::ok(page))
}

/// `GET /api/research/backtest-reports` — paginated backtest-report ledger catalog.
pub async fn list_backtest_reports(
    state: web::Data<AppState>,
    query: web::Query<BacktestReportListQuery>,
) -> Result<WebResponse<Paginated<BacktestReportView>>, WebError> {
    let page = state
        .research_catalog
        .list_backtest_reports(query.into_inner())
        .await?
        .map(BacktestReportView::from);
    Ok(WebResponse::ok(page))
}

/// `GET /api/research/comparison-reports` — paginated comparison-report catalog.
pub async fn list_comparison_reports(
    state: web::Data<AppState>,
    query: web::Query<ComparisonReportListQuery>,
) -> Result<WebResponse<Paginated<ModelComparisonReportView>>, WebError> {
    let page = state
        .research_catalog
        .list_comparison_reports(query.into_inner())
        .await?
        .map(ModelComparisonReportView::from);
    Ok(WebResponse::ok(page))
}

/// `POST /api/research/models/train` — train + register a Candidate version.
pub async fn train(
    state: web::Data<AppState>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<TrainModelRequest>,
) -> Result<WebResponse<TrainedModelView>, WebError> {
    let request = body.into_inner();
    let reason = request.reason.clone();
    let view = state.model_training.train(request).await?;
    state
        .events
        .publish(CoreEvent::MaterializationRun(MaterializationRunEvent {
            run_id: view.model_version_id.to_string(),
            kind: MaterializationRunKind::Training,
            status: MaterializationRunStatus::Completed,
        }));
    op_ctx.set_action(OperationCategory::Other, "model.train");
    op_ctx.set_resource(
        ResourceType::Materialization,
        view.model_version_id.to_string(),
    );
    op_ctx.set_detail(serde_json::json!({
        "model_version_id": view.model_version_id.to_string(),
        "artifact_hash": view.artifact_hash,
        "publication_status": view.publication_status,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }));
    Ok(WebResponse::ok(view))
}

/// `GET /api/research/models/{id}` — registered version (UI polling).
pub async fn get_version(
    state: web::Data<AppState>,
    id: web::Path<ModelVersionId>,
) -> Result<WebResponse<TrainedModelView>, WebError> {
    let info = state
        .model_training
        .find_version(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("model version not found: {id}")))?;
    Ok(WebResponse::ok(TrainedModelView::from(info)))
}

/// `POST /api/research/models/{id}/backtest` — PIT backtest over a dataset.
pub async fn backtest(
    state: web::Data<AppState>,
    id: web::Path<ModelVersionId>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RunBacktestRequest>,
) -> Result<WebResponse<BacktestReportView>, WebError> {
    let request = body.into_inner();
    let reason = request.reason.clone();
    let model_version_id = id.into_inner();
    let view = state.backtests.run(model_version_id, request).await?;
    state
        .events
        .publish(CoreEvent::MaterializationRun(MaterializationRunEvent {
            run_id: view.model_run_id.to_string(),
            kind: MaterializationRunKind::Backtest,
            status: MaterializationRunStatus::Completed,
        }));
    op_ctx.set_action(OperationCategory::Other, "model.backtest");
    op_ctx.set_resource(ResourceType::Replay, view.backtest_report_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "backtest_report_id": view.backtest_report_id.to_string(),
        "model_version_id": view.model_version_id.to_string(),
        "rank_ic": view.rank_ic,
        "hit_rate": view.hit_rate,
        "sample_count": view.sample_count,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }));
    Ok(WebResponse::ok(view))
}

/// `GET /api/research/backtest-reports/{id}` — stored report.
pub async fn get_backtest_report(
    state: web::Data<AppState>,
    id: web::Path<BacktestReportId>,
) -> Result<WebResponse<BacktestReportView>, WebError> {
    let info = state
        .backtests
        .find_report(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("backtest report not found: {id}")))?;
    Ok(WebResponse::ok(BacktestReportView::from(info)))
}

/// `GET /api/research/comparison-reports/{id}` — stored pairwise comparison.
pub async fn get_comparison_report(
    state: web::Data<AppState>,
    id: web::Path<ModelComparisonReportId>,
) -> Result<WebResponse<ModelComparisonReportView>, WebError> {
    let info = state
        .backtests
        .find_comparison_report(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("comparison report not found: {id}")))?;
    Ok(WebResponse::ok(ModelComparisonReportView::from(info)))
}
