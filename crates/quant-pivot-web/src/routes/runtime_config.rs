//! Governance versioned runtime-config endpoints.
//!
//! Runtime configuration changes only through immutable, audited versions —
//! there is no bare in-place mutation. The full activation pipeline:
//!
//! 1. `POST /runtime-config/versions` — typed-parse (`schema_version = 5`,
//!    unknown fields rejected) + semantic validation, canonical JSON, content
//!    hash, immutable row.
//! 2. `POST .../activate` (or `.../rollback`) — re-parse + validate, audited
//!    registry activation (hash chain), then [`RuntimeConfigPort::apply`]
//!    propagates to live subscribers. If the live apply fails after the durable
//!    write, a compensating rollback activation is recorded so the active
//!    version never diverges from the live config.
//!
//! Phase 0: preflight is a no-op; exposure/reservation checks return in Phase 1.
//! Reads mask notification credentials (`bot_token`, `webhook.url`).

use actix_web::{http::Method, web};
use chrono::Utc;
use quant_pivot_error::control::ControlError;
use quant_pivot_models::{
    domain::{
        ActivateRuntimeConfigRequest, CoreEvent, CoreEventPublisher,
        CreateRuntimeConfigVersionRequest, NewRuntimeConfigActivation, NewRuntimeConfigVersion,
        RollbackRuntimeConfigRequest, RuntimeConfigActivationInfo, RuntimeConfigCurrentView,
        RuntimeConfigSchemaView, RuntimeConfigVersionListQuery, RuntimeConfigVersionView,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    hashing::CanonicalDigest,
    runtime_config::{
        RuntimeConfig, apply_runtime_config_patch, build_preferences_schema, mask_config_json,
        unmask_with, validate_runtime_config,
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
            "/runtime-config/schema",
            Rule::ResourceOp(ResourceType::RuntimeConfig, Operation::Read),
            schema,
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

/// `GET /api/runtime-config` — the live (applied) runtime config plus the
/// active version metadata, credentials masked.
pub async fn current(
    state: web::Data<AppState>,
) -> Result<WebResponse<RuntimeConfigCurrentView>, WebError> {
    let activation = state.runtime_config.load_current_activation().await?;
    let version = state.runtime_config.load_current().await?.map(|info| {
        let masked = mask_config_json(&info.config_json);
        RuntimeConfigVersionView::from_info(info, masked)
    });
    let config = state.runtime_config_apply.current().to_masked_json();
    Ok(WebResponse::ok(RuntimeConfigCurrentView {
        version,
        config,
        activation,
    }))
}

/// `GET /api/runtime-config/schema` — preferences envelope for the UI form renderer.
pub async fn schema() -> Result<WebResponse<RuntimeConfigSchemaView>, WebError> {
    Ok(WebResponse::ok(build_preferences_schema()))
}

/// `GET /api/runtime-config/versions` — the immutable version catalog (masked).
pub async fn list_versions(
    state: web::Data<AppState>,
    query: web::Query<RuntimeConfigVersionListQuery>,
) -> Result<WebResponse<Vec<RuntimeConfigVersionView>>, WebError> {
    let limit = query
        .into_inner()
        .limit
        .unwrap_or(DEFAULT_VERSION_LIMIT)
        .min(MAX_VERSION_LIMIT);
    let views = state
        .runtime_config
        .list_versions(limit)
        .await?
        .into_iter()
        .map(|info| {
            let masked = mask_config_json(&info.config_json);
            RuntimeConfigVersionView::from_info(info, masked)
        })
        .collect();
    Ok(WebResponse::ok(views))
}

/// `POST /api/runtime-config/versions` — create an immutable version (governed).
///
/// The body is typed-parsed and semantically validated **before** anything is
/// persisted; the stored JSON is the canonical re-serialization, so the
/// content hash is stable across formatting differences.
pub async fn create_version(
    state: web::Data<AppState>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<CreateRuntimeConfigVersionRequest>,
) -> Result<WebResponse<RuntimeConfigVersionView>, WebError> {
    let body = body.into_inner();
    body.ensure_payload().map_err(WebError::BadRequest)?;
    let current = state.runtime_config_apply.current();
    let config = if let Some(patch) = body.config_patch {
        apply_runtime_config_patch(&current, &patch)
            .map_err(|error| WebError::BadRequest(error.to_string()))?
    } else {
        let mut config_json = body.config_json.expect("validated payload");
        unmask_with(&mut config_json, &current);
        parse_and_validate(&config_json)?
    };

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
        reason: body.reason.clone(),
    };
    let version = state.runtime_config.create_version(version).await?;

    op_ctx.set_action(
        OperationCategory::RuntimeConfig,
        "runtime_config.create_version",
    );
    op_ctx.set_resource(
        ResourceType::RuntimeConfig,
        version.runtime_config_version_id.to_string(),
    );
    op_ctx.set_state_hashes(None, Some(config_hash.as_str().to_owned()));
    op_ctx.set_detail(serde_json::json!({
        "config_hash": config_hash,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
    }));
    let masked = mask_config_json(&version.config_json);
    Ok(WebResponse::ok(RuntimeConfigVersionView::from_info(
        version, masked,
    )))
}

/// `POST /api/runtime-config/versions/{id}/activate` — promote a version and
/// apply it to the live system.
pub async fn activate_version(
    state: web::Data<AppState>,
    id: web::Path<RuntimeConfigVersionId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<ActivateRuntimeConfigRequest>,
) -> Result<WebResponse<RuntimeConfigActivationInfo>, WebError> {
    let version_id = id.into_inner();
    let before_hash = state
        .runtime_config
        .load_current()
        .await?
        .map(|version| version.config_hash.as_str().to_owned());
    let after_hash = state
        .runtime_config
        .load_version(&version_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!("runtime config version not found: {version_id}"))
        })?
        .config_hash
        .as_str()
        .to_owned();
    let audited = transition_version(
        &state,
        version_id,
        &actor,
        acting_role.clone(),
        &request_id,
        body.into_inner().reason,
        RuntimeConfigActivationKind::Promote,
    )
    .await?;
    op_ctx.set_action(OperationCategory::RuntimeConfig, "runtime_config.activate");
    op_ctx.set_state_hashes(before_hash, Some(after_hash));
    record_activation(&op_ctx, &state.events, &audited, &acting_role, &request_id);
    Ok(WebResponse::ok(audited))
}

/// `POST /api/runtime-config/versions/{id}/rollback` — roll back to a version
/// and apply it to the live system.
pub async fn rollback_version(
    state: web::Data<AppState>,
    id: web::Path<RuntimeConfigVersionId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RollbackRuntimeConfigRequest>,
) -> Result<WebResponse<RuntimeConfigActivationInfo>, WebError> {
    let version_id = id.into_inner();
    let before_hash = state
        .runtime_config
        .load_current()
        .await?
        .map(|version| version.config_hash.as_str().to_owned());
    let after_hash = state
        .runtime_config
        .load_version(&version_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!("runtime config version not found: {version_id}"))
        })?
        .config_hash
        .as_str()
        .to_owned();
    let audited = transition_version(
        &state,
        version_id,
        &actor,
        acting_role.clone(),
        &request_id,
        body.into_inner().reason,
        RuntimeConfigActivationKind::Rollback,
    )
    .await?;
    op_ctx.set_action(OperationCategory::RuntimeConfig, "runtime_config.rollback");
    op_ctx.set_state_hashes(before_hash, Some(after_hash));
    record_activation(&op_ctx, &state.events, &audited, &acting_role, &request_id);
    Ok(WebResponse::ok(audited))
}

/// Typed parse + semantic validation (fail-closed, HTTP 400 on any error).
fn parse_and_validate(config_json: &serde_json::Value) -> Result<RuntimeConfig, WebError> {
    let config = RuntimeConfig::from_json(config_json)
        .map_err(|error| WebError::BadRequest(error.to_string()))?;
    let report = validate_runtime_config(&config);
    if report.has_errors() {
        return Err(WebError::BadRequest(report.to_string()));
    }
    Ok(config)
}

/// Shared activate/rollback path: typed-parse + validate the target version,
/// preflight against the live money state, write the durable audited
/// activation, then apply to the live system.
async fn transition_version(
    state: &AppState,
    version_id: RuntimeConfigVersionId,
    actor: &AuthedActor,
    acting_role: ActingRole,
    request_id: &RequestId,
    reason: String,
    kind: RuntimeConfigActivationKind,
) -> Result<RuntimeConfigActivationInfo, WebError> {
    let Some(version) = state.runtime_config.load_version(&version_id).await? else {
        return Err(WebError::NotFound(format!(
            "runtime config version not found: {version_id}"
        )));
    };
    let config = parse_and_validate(&version.config_json)?;
    // Live money-state preflight (mode validity + exposure ceilings vs
    // in-flight reservations). Rejecting here leaves DB + live state untouched.
    state.runtime_config_apply.preflight(&config)?;

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
        activated_by: actor.claims.username.clone(),
        reason: reason.clone(),
        activation_kind: kind,
        previous_runtime_config_version_id: previous.clone(),
        rollback_target_version_id: rollback_target,
        // The registry assigns this from the chained audit event it appends.
        audit_event_id: None,
    };
    let activation = state.runtime_config.activate_version(activation).await?;

    // Propagate to the live system. The applicator re-preflights against the
    // money state at this exact moment; a failure here means the durable
    // activation exists but the live system kept the previous configuration —
    // compensate durably so the active version and the live config never
    // diverge.
    if let Err(error) = state.runtime_config_apply.apply(config).await {
        return Err(revert_unapplied_activation(
            state,
            actor,
            acting_role,
            request_id,
            &activation,
            previous,
            &error,
        )
        .await);
    }

    Ok(activation)
}

/// Compensate a durable activation whose live apply failed.
///
/// The live system never left the previous configuration, so consistency is
/// restored on the durable side: a compensating `Rollback` activation
/// re-points the active version at the previous one, keeping the audit chain
/// truthful — both the failed promotion and its automatic revert are
/// recorded. Returns the conflict error to surface to the operator.
async fn revert_unapplied_activation(
    state: &AppState,
    actor: &AuthedActor,
    acting_role: ActingRole,
    request_id: &RequestId,
    failed: &RuntimeConfigActivationInfo,
    previous: Option<RuntimeConfigVersionId>,
    apply_error: &ControlError,
) -> WebError {
    let failed_version = failed.runtime_config_version_id.clone();
    let Some(previous_id) = previous else {
        tracing::error!(
            %apply_error,
            version_id = %failed_version,
            "runtime config activation not applied and no previous version exists to revert to"
        );
        return WebError::Conflict(format!(
            "activation recorded but not applied to the live system: {apply_error}; \
             no previous version exists to auto-revert to — re-activate manually"
        ));
    };

    let reason = format!(
        "auto-revert: live apply of version {failed_version} failed: {apply_error}; \
         acting_role={}; request_id={}",
        acting_role.0, request_id.0
    );
    let activation = NewRuntimeConfigActivation {
        runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
        runtime_config_version_id: previous_id.clone(),
        activated_at: Utc::now(),
        activated_by: actor.claims.username.clone(),
        reason: reason.clone(),
        activation_kind: RuntimeConfigActivationKind::Rollback,
        previous_runtime_config_version_id: Some(failed_version.clone()),
        rollback_target_version_id: Some(previous_id),
        audit_event_id: None,
    };
    match state.runtime_config.activate_version(activation).await {
        Ok(_) => {
            tracing::warn!(
                %apply_error,
                version_id = %failed_version,
                acting_role = %acting_role.0,
                request_id = %request_id.0,
                "runtime config activation not applied; durable state auto-reverted"
            );
            WebError::Conflict(format!(
                "activation was not applied to the live system: {apply_error}; \
                 the durable activation was automatically reverted and the \
                 previous configuration remains in effect"
            ))
        }
        Err(revert_error) => {
            tracing::error!(
                %apply_error,
                %revert_error,
                version_id = %failed_version,
                acting_role = %acting_role.0,
                request_id = %request_id.0,
                "runtime config activation not applied AND auto-revert failed — \
                 active version diverges from the live config until rolled back"
            );
            WebError::Conflict(format!(
                "activation recorded but not applied ({apply_error}) and the \
                 automatic revert failed ({revert_error}); the live system keeps \
                 running the previous configuration — roll back manually"
            ))
        }
    }
}

/// Stamp the operation log for a runtime-config activation, linking the chained
/// audit event the registry assigned to the activation row.
fn record_activation(
    op_ctx: &OperationCtx,
    events: &CoreEventPublisher,
    activation: &RuntimeConfigActivationInfo,
    acting_role: &ActingRole,
    request_id: &RequestId,
) {
    op_ctx.set_resource(
        ResourceType::RuntimeConfig,
        activation.runtime_config_version_id.to_string(),
    );
    op_ctx.set_detail(serde_json::json!({
        "activation_kind": activation.activation_kind,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "runtime_config_activation_id": activation.runtime_config_activation_id,
        "runtime_config_version_id": activation.runtime_config_version_id,
        "previous_runtime_config_version_id": activation.previous_runtime_config_version_id,
        "rollback_target_version_id": activation.rollback_target_version_id,
    }));
    events.publish(CoreEvent::ConfigActivated {
        version_id: activation.runtime_config_version_id.to_string(),
    });
}
