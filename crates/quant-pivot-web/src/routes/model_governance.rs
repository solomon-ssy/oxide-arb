//! Model publish / rollback admin endpoints (Phase 3.7).
//!
//! Money-critical lifecycle transitions are gated by quality gates + shadow
//! stability in core; these routes add Casbin role enforcement and HTTP audit.

use actix_web::{http::Method, web};
use quant_pivot_models::{
    domain::{
        GovernanceActor, PublishModelCommand, PublishModelRequest, RollbackModelCommand,
        RollbackModelRequest, TrainedModelView,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
    },
    types::ModelVersionId,
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
    let view = state
        .model_governance
        .publish(
            PublishModelCommand {
                model_version_id: model_version_id.clone(),
                reason: request.reason.clone(),
            },
            governance_actor(&actor, &acting_role),
        )
        .await
        .map(TrainedModelView::from)?;
    op_ctx.set_action(OperationCategory::Governance, "research.model.publish");
    op_ctx.set_resource(ResourceType::Publication, model_version_id.to_string());
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
    let view = state
        .model_governance
        .rollback(
            RollbackModelCommand {
                model_version_id: model_version_id.clone(),
                reason: request.reason.clone(),
            },
            governance_actor(&actor, &acting_role),
        )
        .await
        .map(TrainedModelView::from)?;
    op_ctx.set_action(OperationCategory::Governance, "research.model.rollback");
    op_ctx.set_resource(ResourceType::Publication, model_version_id.to_string());
    op_ctx.set_detail(serde_json::json!({
        "restored_model_version_id": view.model_version_id.to_string(),
        "publication_status": view.publication_status,
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
