//! Model publish / rollback admin endpoints (Phase 3.7).
//!
//! Money-critical lifecycle transitions are gated by quality gates + shadow
//! stability in core; these routes add Casbin role enforcement and HTTP audit.

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        BindCalibrationRequest, GovernanceActor, PublishModelCommand, PublishModelRequest,
        RetireModelCommand, RetireModelRequest, RollbackModelCommand, RollbackModelRequest,
        TrainedModelView,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    hashing::CanonicalDigest,
    types::ModelVersionId,
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

/// Model-governance routes (publish / rollback).
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
            "/research/models/{id}/rollback",
            Rule::ActingRoleGoverned(ResourceType::Publication, Operation::Rollback),
            rollback,
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
    ]
}

/// `POST /api/research/models/{id}/publish` — promote after quality gate + shadow stability.
pub async fn publish(
    state: web::Data<AppState>,
    id: web::Path<ModelVersionId>,
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
            governance_actor(&actor, &acting_role),
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
    }));
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/models/{id}/rollback` — retire published version, restore predecessor.
pub async fn rollback(
    state: web::Data<AppState>,
    id: web::Path<ModelVersionId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RollbackModelRequest>,
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
    let restored = state
        .model_governance
        .rollback(
            RollbackModelCommand {
                model_version_id: model_version_id.clone(),
                reason: request.reason.clone(),
            },
            governance_actor(&actor, &acting_role),
        )
        .await?;
    let before_hash = canonical_state_hash(&before)?;
    let after_hash = canonical_state_hash(&restored)?;
    let view = TrainedModelView::from(restored);
    op_ctx.set_action(OperationCategory::Governance, "model.rollback");
    op_ctx.set_resource(ResourceType::Publication, model_version_id.to_string());
    op_ctx.set_state_hashes(Some(before_hash), Some(after_hash));
    op_ctx.set_detail(serde_json::json!({
        "restored_model_version_id": view.model_version_id.to_string(),
        "publication_status": view.publication_status,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "reason": request.reason,
    }));
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/models/{id}/retire` — retire a published version without restore.
pub async fn retire(
    state: web::Data<AppState>,
    id: web::Path<ModelVersionId>,
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
            governance_actor(&actor, &acting_role),
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
    }));
    Ok(WebResponse::ok(view))
}

/// `POST /api/research/models/{id}/bind-calibration` — bind a calibrator artifact.
pub async fn bind_calibration(
    state: web::Data<AppState>,
    id: web::Path<ModelVersionId>,
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
            governance_actor(&actor, &acting_role),
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
    }));
    Ok(WebResponse::ok(view))
}

fn governance_actor(actor: &AuthedActor, acting_role: &ActingRole) -> GovernanceActor {
    GovernanceActor {
        username: actor.claims.username.clone(),
        role: Some(acting_role.0.clone()),
    }
}

fn canonical_state_hash<T: Serialize>(state: &T) -> Result<String, WebError> {
    CanonicalDigest::content_hash_json(state)
        .map(|hash| hash.as_str().to_owned())
        .map_err(|error| WebError::Internal(format!("canonical state hash failed: {error}")))
}
