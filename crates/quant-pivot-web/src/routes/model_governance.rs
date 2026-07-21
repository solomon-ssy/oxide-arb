//! Model publication lifecycle admin endpoints.
//!
//! Money-critical lifecycle transitions are gated by quality gates + shadow
//! stability in core; these routes add Casbin role enforcement and HTTP audit.

use actix_web::{
    http::Method,
    web::{Data, Path, Query},
};
use quant_pivot_models::{
    domain::{
        api::{
            BindCalibrationRequest, BindPublishPathSetRequest, ModelCalibrationFitPreflightQuery,
            ModelCalibrationFitPreflightView, PublishModelRequest, RetireModelRequest,
            TrainedModelView,
        },
        ports::{GovernanceActor, PublishModelCommand, RetireModelCommand},
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    hashing::CanonicalDigest,
    types::{ContentHash, ModelVersionId, RoleCode, UserId},
};
use serde::Serialize;

use crate::{
    audit::OperationCtx,
    auth::casbin::Rule,
    error::WebError,
    extractors::{ActingRole, AuthedActor, RequestId, ValidatedJson},
    response::WebResponse,
    routes::registry::{RouteSpec, spec},
    state::AppState,
};

/// Model-governance routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::POST,
            "/research/models/{id}/publish",
            Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Publish),
            publish,
        ),
        spec(
            Method::POST,
            "/research/models/{id}/retire",
            Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Retire),
            retire,
        ),
        spec(
            Method::POST,
            "/research/models/{id}/bind-calibration",
            Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Create),
            bind_calibration,
        ),
        spec(
            Method::POST,
            "/research/models/{id}/bind-publish-path-set",
            Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Create),
            bind_publish_path_set,
        ),
        spec(
            Method::GET,
            "/research/models/{id}/calibration-fit-preflight",
            Rule::ResourceOp(ResourceType::Materialization, Operation::Read),
            calibration_fit_preflight,
        ),
    ]
}

/// `POST /api/research/models/{id}/publish` — promote after quality gate + shadow stability.
pub async fn publish(
    state: Data<AppState>,
    id: Path<ModelVersionId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<PublishModelRequest>,
) -> Result<WebResponse<TrainedModelView>, WebError> {
    let request = body.into_inner();
    let model_version_id = id.into_inner();
    let before = state
        .model_training
        .find_version(&model_version_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!("model_version not found: {model_version_id}"))
        })?;
    let published = state
        .model_governance
        .publish(
            PublishModelCommand {
                model_version_id: model_version_id.clone(),
                reason: request.reason.clone(),
            },
            governance_actor(&actor, &acting_role)?,
        )
        .await?;
    let before_hash = canonical_state_hash(&before)?;
    let after_hash = canonical_state_hash(&published)?;
    let view = TrainedModelView::from(published);
    op_ctx.set_action(OperationCategory::Governance, "model.publish");
    op_ctx.set_resource(ResourceType::Publication, model_version_id.to_string());
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "model_version_id": view.model_version_id.to_string(),
        "publication_status": view.publication_status,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }))?;
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/models/{id}/retire` — retire a published version without restore.
pub async fn retire(
    state: Data<AppState>,
    id: Path<ModelVersionId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RetireModelRequest>,
) -> Result<WebResponse<TrainedModelView>, WebError> {
    let request = body.into_inner();
    let model_version_id = id.into_inner();
    let before = state
        .model_training
        .find_version(&model_version_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!("model_version not found: {model_version_id}"))
        })?;
    let retired = state
        .model_governance
        .retire(
            RetireModelCommand {
                model_version_id: model_version_id.clone(),
                reason: request.reason.clone(),
            },
            governance_actor(&actor, &acting_role)?,
        )
        .await?;
    let before_hash = canonical_state_hash(&before)?;
    let after_hash = canonical_state_hash(&retired)?;
    let view = TrainedModelView::from(retired);
    op_ctx.set_action(OperationCategory::Governance, "model.retire");
    op_ctx.set_resource(ResourceType::Publication, model_version_id.to_string());
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "model_version_id": view.model_version_id.to_string(),
        "publication_status": view.publication_status,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }))?;
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/models/{id}/bind-publish-path-set` — pin CPCV path set for publish gates.
pub async fn bind_publish_path_set(
    state: Data<AppState>,
    id: Path<ModelVersionId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<BindPublishPathSetRequest>,
) -> Result<WebResponse<TrainedModelView>, WebError> {
    let request = body.into_inner();
    let model_version_id = id.into_inner();
    let before = state
        .model_training
        .find_version(&model_version_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!("model_version not found: {model_version_id}"))
        })?;
    let updated = state
        .model_governance
        .bind_publish_path_set(
            &model_version_id,
            request.clone(),
            governance_actor(&actor, &acting_role)?,
        )
        .await?;
    let before_hash = canonical_state_hash(&before)?;
    let after_hash = canonical_state_hash(&updated)?;
    let view = TrainedModelView::from(updated);
    op_ctx.set_action(OperationCategory::Governance, "model.bind_publish_path_set");
    op_ctx.set_resource(ResourceType::Publication, model_version_id.to_string());
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "model_version_id": view.model_version_id.to_string(),
        "path_set_id": request.path_set_id.to_string(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }))?;
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/models/{id}/bind-calibration` — bind a calibrator artifact.
pub async fn bind_calibration(
    state: Data<AppState>,
    id: Path<ModelVersionId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<BindCalibrationRequest>,
) -> Result<WebResponse<TrainedModelView>, WebError> {
    let request = body.into_inner();
    let model_version_id = id.into_inner();
    let before = state
        .model_training
        .find_version(&model_version_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!("model_version not found: {model_version_id}"))
        })?;
    let created = state
        .model_governance
        .bind_calibration(
            &model_version_id,
            request.clone(),
            governance_actor(&actor, &acting_role)?,
        )
        .await?;
    let before_hash = canonical_state_hash(&before)?;
    let after_hash = canonical_state_hash(&created)?;
    let view = TrainedModelView::from(created);
    op_ctx.set_action(OperationCategory::Governance, "model.bind_calibration");
    op_ctx.set_resource(ResourceType::Publication, view.model_version_id.to_string());
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "source_model_version_id": model_version_id.to_string(),
        "calibrated_model_version_id": view.model_version_id.to_string(),
        "calibrator_ref": request.calibrator_ref.to_string(),
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }))?;
    Ok(WebResponse::ok(view))
}

/// `GET /api/research/models/{id}/calibration-fit-preflight` — read-only
/// disjoint + embargo check for the "Fit Model Calibrator" wizard.
///
/// Surfaced live as the operator picks a model/dataset pair. Never enqueues a
/// job or mutates state.
pub async fn calibration_fit_preflight(
    state: Data<AppState>,
    id: Path<ModelVersionId>,
    query: Query<ModelCalibrationFitPreflightQuery>,
) -> Result<WebResponse<ModelCalibrationFitPreflightView>, WebError> {
    let view = state
        .model_calibration_fit
        .preflight(&id.into_inner(), &query.into_inner().calibration_dataset_id)
        .await?;
    Ok(WebResponse::ok(view))
}

fn governance_actor(
    actor: &AuthedActor,
    acting_role: &ActingRole,
) -> Result<GovernanceActor, WebError> {
    let user_id = actor.claims.sub.parse::<UserId>().map_err(|error| {
        WebError::Internal(format!("authenticated subject is invalid: {error}"))
    })?;
    Ok(GovernanceActor::authenticated(
        user_id,
        actor.claims.username.clone(),
        RoleCode::new(acting_role.0.clone()),
    ))
}

fn canonical_state_hash<T: Serialize>(state: &T) -> Result<ContentHash, WebError> {
    CanonicalDigest::content_hash_json(state)
        .map_err(|error| WebError::Internal(format!("canonical state hash failed: {error}")))
}
