//! Governance versioned runtime-config endpoints.
//!
//! Runtime configuration changes only through immutable, audited versions —
//! there is no bare in-place mutation. The full activation pipeline:
//!
//! 1. `POST /runtime-config/versions` — typed-parse (exact current schema version,
//!    unknown fields rejected) + semantic validation, canonical JSON, content
//!    hash, immutable row.
//! 2. `POST .../activate` (or `.../rollback`) — prepare every fallible live
//!    dependency, atomically append the approved activation, then publish the
//!    already-prepared immutable snapshot.
//!
//! Phase 0: preflight is a no-op; exposure/reservation checks return in Phase 1.
//! Reads mask notification credentials (`bot_token`, `webhook.url`).

use actix_web::{http::Method, web};
use chrono::Utc;
use quant_pivot_models::{
    domain::{
        ActivateRuntimeConfigRequest, CoreEvent, CoreEventPublisher,
        CreateRuntimeConfigVersionRequest, NewRuntimeConfigActivation, NewRuntimeConfigApproval,
        NewRuntimeConfigVersion, RecordRuntimeConfigApprovalRequest, RollbackRuntimeConfigRequest,
        RuntimeConfigActivationInfo, RuntimeConfigApprovalInfo, RuntimeConfigCurrentView,
        RuntimeConfigSchemaView, RuntimeConfigVersionListQuery, RuntimeConfigVersionView,
        SchedulePreviewRequest, SchedulePreviewView,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    hashing::CanonicalDigest,
    runtime_config::{
        RuntimeConfig, apply_runtime_config_patch, build_preferences_schema, mask_config_json,
        preview_fire_times, unmask_with, validate_runtime_config,
    },
    types::{RuntimeConfigActivationId, RuntimeConfigApprovalId, RuntimeConfigVersionId},
};
use serde_json::Value;
use std::collections::BTreeMap;

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
            Method::POST,
            "/runtime-config/schedule-preview",
            Rule::ResourceOp(ResourceType::RuntimeConfig, Operation::Read),
            schedule_preview,
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
            Method::GET,
            "/runtime-config/approvals",
            Rule::ResourceOp(ResourceType::RuntimeConfig, Operation::Read),
            list_valid_approvals,
        ),
        spec(
            Method::POST,
            "/runtime-config/versions/{id}/approvals",
            Rule::ActingRoleGoverned(ResourceType::RuntimeConfig, Operation::Approve),
            record_approval,
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

/// `GET /api/runtime-config/approvals` — approvals currently eligible for activation.
pub async fn list_valid_approvals(
    state: web::Data<AppState>,
) -> Result<WebResponse<Vec<RuntimeConfigApprovalInfo>>, WebError> {
    Ok(WebResponse::ok(
        state
            .runtime_config
            .list_valid_approvals(MAX_VERSION_LIMIT)
            .await?,
    ))
}

/// `POST /api/runtime-config/versions/{id}/approvals` — append a WORM decision.
pub async fn record_approval(
    state: web::Data<AppState>,
    id: web::Path<RuntimeConfigVersionId>,
    actor: AuthedActor,
    acting_role: ActingRole,
    request_id: RequestId,
    op_ctx: OperationCtx,
    body: ValidatedJson<RecordRuntimeConfigApprovalRequest>,
) -> Result<WebResponse<RuntimeConfigApprovalInfo>, WebError> {
    let version_id = id.into_inner();
    let request = body.into_inner();
    if request
        .expires_at
        .is_some_and(|expires_at| expires_at <= Utc::now())
    {
        return Err(WebError::BadRequest(
            "approval expiry must be in the future".to_owned(),
        ));
    }
    let version = state
        .runtime_config
        .load_version(&version_id)
        .await?
        .ok_or_else(|| {
            WebError::NotFound(format!("runtime config version not found: {version_id}"))
        })?;
    let approval = state
        .runtime_config
        .record_approval(NewRuntimeConfigApproval {
            runtime_config_approval_id: RuntimeConfigApprovalId::from_v7(),
            runtime_config_version_id: version.runtime_config_version_id,
            config_hash: version.config_hash.clone(),
            decision: request.decision,
            decided_by: actor.claims.username.clone(),
            reason: request.reason,
            decided_at: Utc::now(),
            expires_at: request.expires_at,
        })
        .await?;
    op_ctx.set_action(OperationCategory::RuntimeConfig, "runtime_config.approval");
    op_ctx.set_resource(
        ResourceType::RuntimeConfig,
        approval.runtime_config_version_id.to_string(),
    );
    op_ctx.set_state_hashes(None, Some(approval.config_hash.to_string()));
    op_ctx.set_detail(serde_json::json!({
        "runtime_config_approval_id": approval.runtime_config_approval_id,
        "decision": approval.decision,
        "acting_role": acting_role.0,
        "request_id": request_id.0,
        "expires_at": approval.expires_at,
    }));
    Ok(WebResponse::ok(approval))
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

/// `POST /api/runtime-config/schedule-preview` — next fire times for a cadence.
///
/// Stateless dry-run using the same cron parser as the report scheduler; a bad
/// cadence (zero interval, malformed cron, unknown timezone) fails as 400.
pub async fn schedule_preview(
    body: ValidatedJson<SchedulePreviewRequest>,
) -> Result<WebResponse<SchedulePreviewView>, WebError> {
    let body = body.into_inner();
    let next_fire_times = preview_fire_times(&body.cadence, Utc::now(), usize::from(body.count))
        .map_err(|error| WebError::BadRequest(error.to_string()))?;
    Ok(WebResponse::ok(SchedulePreviewView { next_fire_times }))
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
    let config = resolve_create_config(&current, body.config_patch, body.config_json)?;

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
    let request = body.into_inner();
    let audited = transition_version(
        &state,
        version_id,
        &actor,
        request.runtime_config_approval_id,
        request.reason,
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
    let request = body.into_inner();
    let audited = transition_version(
        &state,
        version_id,
        &actor,
        request.runtime_config_approval_id,
        request.reason,
        RuntimeConfigActivationKind::Rollback,
    )
    .await?;
    op_ctx.set_action(OperationCategory::RuntimeConfig, "runtime_config.rollback");
    op_ctx.set_state_hashes(before_hash, Some(after_hash));
    record_activation(&op_ctx, &state.events, &audited, &acting_role, &request_id);
    Ok(WebResponse::ok(audited))
}

/// Fail-closed semantic validation shared by create (patch + json) and activate.
fn validate_runtime_config_struct(config: &RuntimeConfig) -> Result<(), WebError> {
    let report = validate_runtime_config(config);
    if report.has_errors() {
        return Err(WebError::BadRequest(report.to_string()));
    }
    Ok(())
}

/// Merge or parse a create payload, then validate before persistence.
fn resolve_create_config(
    current: &RuntimeConfig,
    config_patch: Option<BTreeMap<String, Value>>,
    config_json: Option<Value>,
) -> Result<RuntimeConfig, WebError> {
    let config = if let Some(patch) = config_patch {
        apply_runtime_config_patch(current, &patch)
            .map_err(|error| WebError::BadRequest(error.to_string()))?
    } else {
        let mut config_json = config_json.expect("validated payload");
        unmask_with(&mut config_json, current);
        RuntimeConfig::from_json(&config_json)
            .map_err(|error| WebError::BadRequest(error.to_string()))?
    };
    validate_runtime_config_struct(&config)?;
    Ok(config)
}

/// Typed parse + semantic validation (fail-closed, HTTP 400 on any error).
fn parse_and_validate(config_json: &serde_json::Value) -> Result<RuntimeConfig, WebError> {
    let config = RuntimeConfig::from_json(config_json)
        .map_err(|error| WebError::BadRequest(error.to_string()))?;
    validate_runtime_config_struct(&config)?;
    Ok(config)
}

/// Shared activate/rollback path: typed-parse + validate the target version,
/// preflight against the live money state, write the durable audited
/// activation, then apply to the live system.
async fn transition_version(
    state: &AppState,
    version_id: RuntimeConfigVersionId,
    actor: &AuthedActor,
    approval_id: RuntimeConfigApprovalId,
    reason: String,
    kind: RuntimeConfigActivationKind,
) -> Result<RuntimeConfigActivationInfo, WebError> {
    let Some(version) = state.runtime_config.load_version(&version_id).await? else {
        return Err(WebError::NotFound(format!(
            "runtime config version not found: {version_id}"
        )));
    };
    let config = parse_and_validate(&version.config_json)?;
    let prepared = state.runtime_config_apply.prepare(config).await?;

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
        runtime_config_approval_id: Some(approval_id),
        activated_by: actor.claims.username.clone(),
        reason: reason.clone(),
        activation_kind: kind,
        previous_runtime_config_version_id: previous.clone(),
        rollback_target_version_id: rollback_target,
        // The registry assigns this from the chained audit event it appends.
        audit_event_id: None,
    };
    let activation = state
        .runtime_config
        .activate_approved_version(
            activation,
            state
                .deploy
                .quant
                .governance
                .require_approver_activator_separation,
        )
        .await?;

    prepared.publish();

    Ok(activation)
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
