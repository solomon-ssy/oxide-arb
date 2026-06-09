//! Governance versioned runtime-config endpoints.
//!
//! Runtime configuration changes only through immutable, audited versions —
//! there is no bare in-place mutation. Reads (`RuntimeConfig:Read`) return the
//! current activation and the version catalog. Mutations are `ActingRoleGoverned`
//! and delegate to the [`ControlFactorRegistry`], which writes the audit hash
//! chain transactionally; the appended event is linked onto the operation log.
//! After activation, the live snapshot refresher is woken by the registry's
//! notify handle (wired in the bootstrap), so no extra propagation is needed here.

use actix_web::{http::Method, web};
use chrono::Utc;
use oxide_arb_models::{
    domain::{
        ActivateRuntimeConfigRequest, CreateRuntimeConfigVersionRequest,
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, RollbackRuntimeConfigRequest,
        RuntimeConfigActivationInfo, RuntimeConfigVersionInfo, RuntimeConfigVersionListQuery,
        control_factor::AuditActor, runtime_config_hash,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    types::{RuntimeConfigActivationId, RuntimeConfigVersionId},
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

/// Default schema version applied when the request omits it.
const DEFAULT_SCHEMA_VERSION: i32 = 1;
/// Default / maximum versions returned by the catalog list.
const DEFAULT_VERSION_LIMIT: u64 = 50;
const MAX_VERSION_LIMIT: u64 = 200;

/// Runtime-config governance routes.
pub(crate) fn route_specs() -> Vec<RouteSpec> {
    vec![
        spec(
            Method::GET,
            "/runtime-config",
            Rule::ResourceOp(ResourceType::RuntimeConfig, Operation::Read),
            current,
        ),
        spec(
            Method::GET,
            "/runtime-config/versions",
            Rule::ResourceOp(ResourceType::RuntimeConfig, Operation::Read),
            list_versions,
        ),
        spec(
            Method::POST,
            "/runtime-config/versions",
            Rule::ActingRoleGoverned(ResourceType::RuntimeConfig, Operation::Create),
            create_version,
        ),
        spec(
            Method::POST,
            "/runtime-config/versions/{id}/activate",
            Rule::ActingRoleGoverned(ResourceType::RuntimeConfig, Operation::Activate),
            activate_version,
        ),
        spec(
            Method::POST,
            "/runtime-config/versions/{id}/rollback",
            Rule::ActingRoleGoverned(ResourceType::RuntimeConfig, Operation::Rollback),
            rollback_version,
        ),
    ]
}

/// `GET /api/runtime-config` — the currently active runtime-config version.
pub async fn current(
    state: web::Data<AppState>,
) -> Result<WebResponse<RuntimeConfigVersionInfo>, WebError> {
    let current = state
        .runtime_config
        .load_current()
        .await?
        .ok_or_else(|| WebError::NotFound("no active runtime config version".to_owned()))?;
    Ok(WebResponse::ok(current))
}

/// `GET /api/runtime-config/versions` — the immutable version catalog.
pub async fn list_versions(
    state: web::Data<AppState>,
    query: web::Query<RuntimeConfigVersionListQuery>,
) -> Result<WebResponse<Vec<RuntimeConfigVersionInfo>>, WebError> {
    let limit = query
        .into_inner()
        .limit
        .unwrap_or(DEFAULT_VERSION_LIMIT)
        .min(MAX_VERSION_LIMIT);
    Ok(WebResponse::ok(
        state.runtime_config.list_versions(limit).await?,
    ))
}

/// `POST /api/runtime-config/versions` — create an immutable version (governed).
pub async fn create_version(
    state: web::Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<CreateRuntimeConfigVersionRequest>,
) -> Result<WebResponse<RuntimeConfigVersionInfo>, WebError> {
    let body = body.into_inner();
    let config_hash = runtime_config_hash(&body.config_json);
    let version = NewRuntimeConfigVersion {
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        config_hash: config_hash.clone(),
        schema_version: body.schema_version.unwrap_or(DEFAULT_SCHEMA_VERSION),
        config_json: body.config_json,
        source: RuntimeConfigVersionSource::Operator,
        created_by: actor.claims.sub.clone(),
        reason: body.reason.clone(),
    };
    let envelope = governance_envelope(&actor, acting_role, &request_id, body.reason);
    let outcome = state
        .registry
        .create_runtime_config_version(envelope, version)
        .await?;

    op_ctx.set_action(
        OperationCategory::RuntimeConfig,
        "runtime_config.create_version",
    );
    op_ctx.set_resource(
        ResourceType::RuntimeConfig,
        outcome.value.runtime_config_version_id.to_string(),
    );
    op_ctx.set_detail(serde_json::json!({ "config_hash": config_hash }));
    op_ctx.link_governance(outcome.audit_event_id);
    Ok(WebResponse::ok(outcome.value))
}

/// `POST /api/runtime-config/versions/{id}/activate` — promote a version.
pub async fn activate_version(
    state: web::Data<AppState>,
    id: web::Path<RuntimeConfigVersionId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<ActivateRuntimeConfigRequest>,
) -> Result<WebResponse<RuntimeConfigActivationInfo>, WebError> {
    let outcome = transition_version(
        &state,
        id.into_inner(),
        &actor,
        acting_role,
        &request_id,
        body.into_inner().reason,
        RuntimeConfigActivationKind::Promote,
    )
    .await?;
    op_ctx.set_action(OperationCategory::RuntimeConfig, "runtime_config.activate");
    record_activation(&op_ctx, &outcome);
    Ok(WebResponse::ok(outcome))
}

/// `POST /api/runtime-config/versions/{id}/rollback` — roll back to a version.
pub async fn rollback_version(
    state: web::Data<AppState>,
    id: web::Path<RuntimeConfigVersionId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RollbackRuntimeConfigRequest>,
) -> Result<WebResponse<RuntimeConfigActivationInfo>, WebError> {
    let outcome = transition_version(
        &state,
        id.into_inner(),
        &actor,
        acting_role,
        &request_id,
        body.into_inner().reason,
        RuntimeConfigActivationKind::Rollback,
    )
    .await?;
    op_ctx.set_action(OperationCategory::RuntimeConfig, "runtime_config.rollback");
    record_activation(&op_ctx, &outcome);
    Ok(WebResponse::ok(outcome))
}

/// Shared activate/rollback path: verify the target version exists, build the
/// activation referencing the current active version as predecessor, and
/// delegate to the registry (which appends the audit event).
async fn transition_version(
    state: &AppState,
    version_id: RuntimeConfigVersionId,
    actor: &AuthedActor,
    acting_role: ActingRole,
    request_id: &RequestId,
    reason: String,
    kind: RuntimeConfigActivationKind,
) -> Result<RuntimeConfigActivationInfo, WebError> {
    if state
        .runtime_config
        .load_version(&version_id)
        .await?
        .is_none()
    {
        return Err(WebError::NotFound(format!(
            "runtime config version not found: {version_id}"
        )));
    }
    let previous = state
        .runtime_config
        .load_current()
        .await?
        .map(|version| version.runtime_config_version_id);
    let rollback_target =
        matches!(kind, RuntimeConfigActivationKind::Rollback).then(|| version_id.clone());

    let activation = NewRuntimeConfigActivation {
        runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
        runtime_config_version_id: version_id,
        activated_at: Utc::now(),
        activated_by: actor.claims.sub.clone(),
        reason: reason.clone(),
        activation_kind: kind,
        previous_runtime_config_version_id: previous,
        rollback_target_version_id: rollback_target,
        // The registry assigns this from the chained audit event it appends.
        audit_event_id: None,
    };
    let envelope = governance_envelope(actor, acting_role, request_id, reason);
    Ok(state
        .registry
        .activate_runtime_config_version(envelope, activation)
        .await?)
}

/// Stamp the operation log for a runtime-config activation, linking the chained
/// audit event the registry assigned to the activation row.
fn record_activation(op_ctx: &OperationCtx, activation: &RuntimeConfigActivationInfo) {
    op_ctx.set_resource(
        ResourceType::RuntimeConfig,
        activation.runtime_config_version_id.to_string(),
    );
    op_ctx.set_detail(serde_json::json!({
        "activation_kind": activation.activation_kind,
    }));
    if let Some(event_id) = activation.audit_event_id.clone() {
        op_ctx.link_governance(event_id);
    }
}

/// Assemble the governance audit envelope from the request-scoped attributes.
fn governance_envelope(
    actor: &AuthedActor,
    acting_role: ActingRole,
    request_id: &RequestId,
    reason: String,
) -> AuditActor {
    AuditActor {
        actor: actor.claims.sub.clone(),
        actor_role: acting_role.0,
        request_id: request_id.0.clone(),
        reason,
    }
}
