//! Governance versioned runtime-config endpoints.
//!
//! Runtime configuration changes only through immutable, audited versions —
//! there is no bare in-place mutation. The full activation pipeline:
//!
//! 1. `POST /runtime-config/versions` — typed-parse (`schema_version = 1`,
//!    unknown fields rejected) + semantic validation, canonical JSON, content
//!    hash, immutable row.
//! 2. `POST .../activate` (or `.../rollback`) — re-parse + validate, live
//!    money-state **preflight** (exposure ceilings vs in-flight reservations),
//!    audited registry activation (hash chain), then
//!    [`RuntimeConfigPort::apply`] propagates to every live subscriber (risk
//!    engine first) so the activation takes effect immediately. If the live
//!    apply fails after the durable write, a compensating rollback activation
//!    is recorded so the active version never diverges from the live config.
//!
//! Reads mask notification credentials (`bot_token`, `webhook.url`).

use actix_web::{http::Method, web};
use chrono::Utc;
use oxide_arb_models::{
    domain::{
        ActivateRuntimeConfigRequest, CoreEvent, CoreEventPublisher,
        CreateRuntimeConfigVersionRequest, JsonValueType, NewRuntimeConfigActivation,
        NewRuntimeConfigVersion, RollbackRuntimeConfigRequest, RuntimeConfigActivationInfo,
        RuntimeConfigCurrentView, RuntimeConfigSchemaFieldView, RuntimeConfigVersionListQuery,
        RuntimeConfigVersionView, RuntimeControlError, control_factor::AuditActor,
        runtime_config_hash,
    },
    enums::{
        operation_log::OperationCategory,
        rbac::{Operation, ResourceType},
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    runtime_config::{RuntimeConfig, mask_config_json, validation::validate_runtime_config},
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
    let version = state.runtime_config.load_current().await?.map(|info| {
        let masked = mask_config_json(&info.config_json);
        RuntimeConfigVersionView::from_info(info, masked)
    });
    let config = state.runtime_config_apply.current().to_masked_json();
    Ok(WebResponse::ok(RuntimeConfigCurrentView {
        version,
        config,
    }))
}

/// `GET /api/runtime-config/schema` — field metadata (path / type / default /
/// description / money-critical / sensitive) for the UI form renderer.
pub async fn schema() -> Result<WebResponse<Vec<RuntimeConfigSchemaFieldView>>, WebError> {
    Ok(WebResponse::ok(build_schema()))
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
    let config = parse_and_validate(&body.config_json)?;

    let config_json = config.to_json();
    let config_hash = runtime_config_hash(&config_json);
    let version = NewRuntimeConfigVersion {
        runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
        config_hash: config_hash.clone(),
        schema_version: config.schema_version,
        config_json,
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
    let masked = mask_config_json(&outcome.value.config_json);
    Ok(WebResponse::ok(RuntimeConfigVersionView::from_info(
        outcome.value,
        masked,
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
    record_activation(&op_ctx, &state.events, &outcome);
    Ok(WebResponse::ok(outcome))
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
    record_activation(&op_ctx, &state.events, &outcome);
    Ok(WebResponse::ok(outcome))
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
        activated_by: actor.claims.sub.clone(),
        reason: reason.clone(),
        activation_kind: kind,
        previous_runtime_config_version_id: previous.clone(),
        rollback_target_version_id: rollback_target,
        // The registry assigns this from the chained audit event it appends.
        audit_event_id: None,
    };
    let envelope = governance_envelope(actor, acting_role.clone(), request_id, reason);
    let outcome = state
        .registry
        .activate_runtime_config_version(envelope, activation)
        .await?;

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
            &outcome,
            previous,
            &error,
        )
        .await);
    }

    Ok(outcome)
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
    apply_error: &RuntimeControlError,
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

    let reason =
        format!("auto-revert: live apply of version {failed_version} failed: {apply_error}");
    let activation = NewRuntimeConfigActivation {
        runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
        runtime_config_version_id: previous_id.clone(),
        activated_at: Utc::now(),
        activated_by: actor.claims.sub.clone(),
        reason: reason.clone(),
        activation_kind: RuntimeConfigActivationKind::Rollback,
        previous_runtime_config_version_id: Some(failed_version.clone()),
        rollback_target_version_id: Some(previous_id),
        audit_event_id: None,
    };
    let envelope = governance_envelope(actor, acting_role, request_id, reason);
    match state
        .registry
        .activate_runtime_config_version(envelope, activation)
        .await
    {
        Ok(_) => {
            tracing::warn!(
                %apply_error,
                version_id = %failed_version,
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
) {
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
    events.publish(CoreEvent::ConfigActivated {
        version_id: activation.runtime_config_version_id.to_string(),
    });
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

// ── Schema metadata ──────────────────────────────────────────────────────────

/// Build the flat field-metadata list by walking the generated JSON Schema.
///
/// Descriptions come from the field Rustdoc on
/// `oxide_arb_models::runtime_config` (schemars maps doc comments to the
/// schema `description`), and the `x-money-critical` / `x-sensitive` keywords
/// are declared next to the field definitions — the schema is the single
/// source of truth, never duplicated here. Each leaf is paired with its
/// compiled-in default from [`RuntimeConfig::default`].
fn build_schema() -> Vec<RuntimeConfigSchemaFieldView> {
    let schema = RuntimeConfig::json_schema();
    let defaults = RuntimeConfig::default().to_json();
    let mut fields = Vec::with_capacity(128);
    walk_schema(&schema, &defaults, String::new(), false, &mut fields);
    fields
}

/// Recursive schema walk: objects with `properties` are sections, everything
/// else (scalars, arrays, open maps) is a leaf field. `money_critical` is
/// inherited from marked containers (e.g. the whole `risk` section) and
/// combined with field-level markers.
fn walk_schema(
    schema: &serde_json::Value,
    default: &serde_json::Value,
    path: String,
    inherited_money_critical: bool,
    fields: &mut Vec<RuntimeConfigSchemaFieldView>,
) {
    let money_critical = inherited_money_critical || schema_flag(schema, "x-money-critical");
    if let Some(properties) = schema.get("properties").and_then(|p| p.as_object()) {
        for (key, child_schema) in properties {
            let child_path = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            let child_default = default.get(key).cloned().unwrap_or(serde_json::Value::Null);
            walk_schema(
                child_schema,
                &child_default,
                child_path,
                money_critical,
                fields,
            );
        }
        return;
    }
    fields.push(RuntimeConfigSchemaFieldView {
        value_type: schema_value_type(schema),
        default: default.clone(),
        description: schema
            .get("description")
            .and_then(|d| d.as_str())
            .unwrap_or_default()
            .to_owned(),
        money_critical,
        sensitive: schema_flag(schema, "x-sensitive"),
        path,
    });
}

/// Whether a boolean `x-` keyword is set on the schema node.
fn schema_flag(schema: &serde_json::Value, flag: &str) -> bool {
    schema.get(flag).and_then(serde_json::Value::as_bool) == Some(true)
}

/// Map the JSON Schema `type` keyword to the UI form value type. Nullable
/// types (`["string", "null"]`) resolve to their non-null member; schemas
/// without a `type` (e.g. enum `oneOf`) render as strings.
fn schema_value_type(schema: &serde_json::Value) -> JsonValueType {
    let type_name = match schema.get("type") {
        Some(serde_json::Value::String(name)) => Some(name.as_str()),
        Some(serde_json::Value::Array(names)) => names
            .iter()
            .filter_map(serde_json::Value::as_str)
            .find(|name| *name != "null"),
        _ => None,
    };
    match type_name {
        Some("number" | "integer") => JsonValueType::Number,
        Some("boolean") => JsonValueType::Boolean,
        Some("array") => JsonValueType::Array,
        Some("object") => JsonValueType::Object,
        _ => JsonValueType::String,
    }
}

#[cfg(test)]
mod tests {
    use super::{JsonValueType, build_schema};

    #[test]
    fn schema_covers_every_leaf_with_a_description() {
        let fields = build_schema();
        assert!(!fields.is_empty());
        let undescribed: Vec<_> = fields
            .iter()
            .filter(|field| field.description.trim().is_empty())
            .map(|field| field.path.clone())
            .collect();
        assert!(
            undescribed.is_empty(),
            "undescribed fields: {undescribed:?}"
        );
    }

    #[test]
    fn sensitive_fields_are_flagged() {
        let fields = build_schema();
        let token = fields
            .iter()
            .find(|f| f.path == "notification.telegram.bot_token")
            .expect("token field present");
        assert!(token.sensitive);
        assert!(
            fields
                .iter()
                .filter(|f| f.sensitive)
                .all(|f| !f.money_critical)
        );
    }

    #[test]
    fn money_critical_sections_propagate_to_leaves() {
        let fields = build_schema();
        let risk_leaves: Vec<_> = fields
            .iter()
            .filter(|f| f.path.starts_with("risk."))
            .collect();
        assert!(!risk_leaves.is_empty());
        assert!(
            risk_leaves.iter().all(|f| f.money_critical),
            "every risk.* leaf inherits the container marker"
        );
        let redeem_leaves: Vec<_> = fields
            .iter()
            .filter(|f| f.path.starts_with("settlement.redeem."))
            .collect();
        assert!(!redeem_leaves.is_empty());
        assert!(redeem_leaves.iter().all(|f| f.money_critical));
        let threshold = fields
            .iter()
            .find(|f| f.path == "detection.endgame.high_threshold")
            .expect("high_threshold present");
        assert!(threshold.money_critical);
    }

    #[test]
    fn value_types_and_defaults_match_the_wire_format() {
        let fields = build_schema();
        let by_path = |path: &str| {
            fields
                .iter()
                .find(|f| f.path == path)
                .unwrap_or_else(|| panic!("missing field {path}"))
        };
        // Decimal money fields are strings on the wire.
        let daily_loss = by_path("risk.max_daily_loss_usd");
        assert_eq!(daily_loss.value_type, JsonValueType::String);
        assert!(daily_loss.default.is_string());
        // Integer cadences are numbers.
        assert_eq!(
            by_path("risk.reservation_ttl_secs").value_type,
            JsonValueType::Number
        );
        // Category weights surface as one object-typed leaf.
        let weights = by_path("detection.endgame.scorer.category_weights");
        assert_eq!(weights.value_type, JsonValueType::Object);
        assert!(weights.default.is_object());
        // Blacklists are arrays.
        assert_eq!(
            by_path("risk.permanent_blacklist_markets").value_type,
            JsonValueType::Array
        );
    }
}
