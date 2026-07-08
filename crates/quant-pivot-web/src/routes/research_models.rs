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
        BacktestReportListQuery, BacktestReportView, ComparisonReportListQuery,
        CreateModelSpecCommand, CreateModelSpecRequest, GovernanceActor, JobSubmitContext,
        ModelComparisonReportView, ModelPublishedCatalogQuery, ModelSpecListQuery,
        ModelVersionListQuery, Paginated, PublishedModelOptionView, QualityGatePreviewQuery,
        QualityGateReportView, QuantModelSpecView, ResearchJobView, RunBacktestRequest,
        TrainModelRequest, TrainedModelView,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    types::{BacktestReportId, ModelComparisonReportId, ModelSpecId, ModelVersionId},
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
            Method::POST,
            "/research/model-specs",
            Rule::ActingRoleGoverned(ResourceType::Materialization, Operation::Create),
            create_model_spec,
        ),
        spec(
            Method::GET,
            "/research/model-specs/{id}",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            get_model_spec,
        ),
        spec(
            Method::GET,
            "/research/models",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list_models,
        ),
        spec(
            Method::GET,
            "/research/models/published-catalog",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            list_published_catalog,
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
            "/research/models/{id}/quality-gate",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            preview_quality_gate,
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

/// `POST /api/research/model-specs` — author a new `draft` model specification.
///
/// The spec is the authoring root of the offline research lifecycle: an operator
/// mints it before planning a training dataset or training a version. Returns
/// `201 Created` with the persisted spec projection.
pub async fn create_model_spec(
    state: web::Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<CreateModelSpecRequest>,
) -> Result<WebResponse<QuantModelSpecView>, WebError> {
    let request = body.into_inner();
    let reason = request.reason.clone();
    let created = state
        .model_spec
        .create(
            CreateModelSpecCommand {
                name: request.name,
                model_family: request.model_family,
                prediction_horizon_secs: request.prediction_horizon_secs,
                feature_schema_version: request.feature_schema_version,
                label_schema_version: request.label_schema_version,
                spec_json: request.spec_json,
                feature_requirements: request.feature_requirements,
                reason: reason.clone(),
            },
            GovernanceActor {
                username: actor.claims.username.clone(),
                role: Some(acting_role.0.clone()),
            },
        )
        .await?;
    let view = QuantModelSpecView::from(created);
    op_ctx.set_action(OperationCategory::Governance, "model_spec.create");
    op_ctx.set_resource(ResourceType::Materialization, view.model_spec_id.clone());
    op_ctx.set_detail(serde_json::json!({
        "model_spec_id": view.model_spec_id,
        "name": view.name,
        "model_family": view.model_family,
        "prediction_horizon_secs": view.prediction_horizon_secs,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }));
    Ok(WebResponse::ok(view))
}

/// `GET /api/research/model-specs/{id}` — single model specification (detail drawer).
pub async fn get_model_spec(
    state: web::Data<AppState>,
    id: web::Path<ModelSpecId>,
) -> Result<WebResponse<QuantModelSpecView>, WebError> {
    let info = state
        .model_spec
        .find(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("model spec not found: {id}")))?;
    Ok(WebResponse::ok(QuantModelSpecView::from(info)))
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

/// `GET /api/research/models/published-catalog` — the `Published`,
/// side-and-category-eligible candidates for one `ModelVersionSelect`
/// runtime-config field (11.2.2 remediation R8).
pub async fn list_published_catalog(
    state: web::Data<AppState>,
    query: web::Query<ModelPublishedCatalogQuery>,
) -> Result<WebResponse<Vec<PublishedModelOptionView>>, WebError> {
    let options = state
        .research_catalog
        .list_published_model_options(query.into_inner())
        .await?;
    Ok(WebResponse::ok(options))
}

/// `GET /api/research/backtest-reports` — paginated backtest-report ledger catalog.
pub async fn list_backtest_reports(
    state: web::Data<AppState>,
    query: web::Query<BacktestReportListQuery>,
) -> Result<WebResponse<Paginated<BacktestReportView>>, WebError> {
    let page = state
        .research_catalog
        .list_backtest_reports(query.into_inner())
        .await?;
    let ids: Vec<_> = page
        .items
        .iter()
        .map(|info| info.backtest_report_id.clone())
        .collect();
    let comparison_ids = state
        .backtests
        .comparison_ids_for_backtest_reports(&ids)
        .await?;
    let items = page
        .items
        .into_iter()
        .map(|info| {
            let comparison_report_id = comparison_ids.get(&info.backtest_report_id).cloned();
            BacktestReportView::from_info(info, comparison_report_id)
        })
        .collect();
    Ok(WebResponse::ok(Paginated {
        items,
        page: page.page,
        size: page.size,
        total: page.total,
        has_next: page.has_next,
    }))
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

/// `POST /api/research/models/train` — enqueue an async training job.
///
/// Returns `202 Accepted` with the queued [`ResearchJobView`]; training runs on
/// the `ResearchJobWorker`. Poll the job / listen on `materialization.run_update`.
pub async fn train(
    state: web::Data<AppState>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<TrainModelRequest>,
) -> Result<WebResponse<ResearchJobView>, WebError> {
    let request = body.into_inner();
    let reason = request.reason.clone();
    let job = state
        .research_jobs
        .enqueue_model_train(
            request,
            JobSubmitContext {
                acting_role: acting_role.0.clone(),
                requested_by: None,
            },
        )
        .await?;
    op_ctx.set_action(OperationCategory::Other, "model.train");
    op_ctx.set_resource(ResourceType::Materialization, job.job_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "job_id": job.job_id.to_string(),
        "kind": "model_train",
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }));
    Ok(WebResponse::accepted(job))
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

/// `GET /api/research/models/{id}/quality-gate` — read-only publish-readiness dry-run.
///
/// Runs the same quality gate as `publish` (no persistence, no state change) and
/// returns the full per-gate scorecard so operators can judge readiness before acting.
pub async fn preview_quality_gate(
    state: web::Data<AppState>,
    id: web::Path<ModelVersionId>,
    query: web::Query<QualityGatePreviewQuery>,
) -> Result<WebResponse<QualityGateReportView>, WebError> {
    let query = query.into_inner();
    let view = state
        .model_governance
        .preview_gate(&id, query.intent, query.backtest_report_id.as_ref())
        .await?;
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/models/{id}/backtest` — enqueue an async PIT backtest job.
///
/// Returns `202 Accepted` with the queued [`ResearchJobView`]; the replay runs on
/// the `ResearchJobWorker`. Poll the job / listen on `materialization.run_update`.
pub async fn backtest(
    state: web::Data<AppState>,
    id: web::Path<ModelVersionId>,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RunBacktestRequest>,
) -> Result<WebResponse<ResearchJobView>, WebError> {
    let request = body.into_inner();
    let reason = request.reason.clone();
    let model_version_id = id.into_inner();
    let job = state
        .research_jobs
        .enqueue_backtest(
            model_version_id,
            request,
            JobSubmitContext {
                acting_role: acting_role.0.clone(),
                requested_by: None,
            },
        )
        .await?;
    op_ctx.set_action(OperationCategory::Other, "model.backtest");
    op_ctx.set_resource(ResourceType::Replay, job.job_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "job_id": job.job_id.to_string(),
        "kind": "backtest",
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": reason,
    }));
    Ok(WebResponse::accepted(job))
}

/// `GET /api/research/backtest-reports/{id}` — stored report.
pub async fn get_backtest_report(
    state: web::Data<AppState>,
    id: web::Path<BacktestReportId>,
) -> Result<WebResponse<BacktestReportView>, WebError> {
    let view = state
        .backtests
        .find_report(&id)
        .await?
        .ok_or_else(|| WebError::NotFound(format!("backtest report not found: {id}")))?;
    Ok(WebResponse::ok(view))
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
