//! Runtime-config model pointer preparation for governed model transitions.

use std::sync::Arc;

use quant_pivot_error::{QuantResult, governance::GovernanceError};
use quant_pivot_models::{
    domain::{
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, PreparedRuntimeConfig,
        RuntimeConfigPort, RuntimeConfigVersionInfo,
    },
    enums::{
        quant::PublicationStatus,
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    hashing::CanonicalDigest,
    runtime_config::{
        ModelVersionRef, RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig, validate_runtime_config,
    },
    types::{ModelVersionId, RuntimeConfigActivationId, RuntimeConfigVersionId},
};
use quant_pivot_repository::traits::{ModelRegistryRepository, RuntimeConfigVersionRepository};

/// Dependencies for synchronizing runtime-config model pointers after governance.
pub struct RuntimeModelPointerSync {
    pub runtime_config_apply: Arc<dyn RuntimeConfigPort>,
    pub runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository>,
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
}

#[derive(Clone, Copy)]
struct ModelPointerRoute {
    is_exit: bool,
}

/// Fully validated pointer activation awaiting the atomic repository commit.
pub struct PreparedProductionPointer {
    pub(crate) snapshot: PreparedRuntimeConfig,
    pub(crate) expected_activation_id: RuntimeConfigActivationId,
    pub(crate) activation: NewRuntimeConfigActivation,
}

/// Immutable rollback pointer/config generation validated before status changes.
pub struct RollbackPointerPreflight {
    pub(crate) prepared: PreparedProductionPointer,
}

/// Prepare an active-model pointer without mutating durable or live state.
pub async fn prepare_production_active(
    deps: &RuntimeModelPointerSync,
    active: &ModelVersionId,
    clear_shadow: bool,
    reason: &str,
    activated_by: &str,
) -> QuantResult<PreparedProductionPointer> {
    let route = ModelPointerRoute {
        is_exit: resolve_is_exit_scorer(deps, active).await?,
    };
    let mut config = (*deps.runtime_config_apply.current()).clone();
    set_production_pointer(&mut config, active, route, clear_shadow);
    prepare_pointer_activation(deps, config, reason, activated_by).await
}

/// Validate and prepare a rollback pointer before model statuses change.
pub async fn preflight_rollback_production_pointer(
    deps: &RuntimeModelPointerSync,
    previous: &ModelVersionId,
    target: &ModelVersionId,
    reason: &str,
    activated_by: &str,
) -> QuantResult<RollbackPointerPreflight> {
    let previous_is_exit = resolve_is_exit_scorer(deps, previous).await?;
    let target_is_exit = resolve_is_exit_scorer(deps, target).await?;
    if previous_is_exit != target_is_exit {
        return Err(GovernanceError::IllegalTransition {
            detail: "rollback current and target resolve to different model pointer routes"
                .to_owned(),
        }
        .into());
    }
    let route = ModelPointerRoute {
        is_exit: target_is_exit,
    };
    let previous_config = (*deps.runtime_config_apply.current()).clone();
    if !config_points_to(&previous_config, previous, route) {
        return Err(GovernanceError::IllegalTransition {
            detail: format!("live runtime config does not point to rollback current {previous}"),
        }
        .into());
    }
    if !pointer_postcondition(deps, previous, route)
        .await
        .map_err(|detail| GovernanceError::IllegalTransition { detail })?
    {
        return Err(GovernanceError::IllegalTransition {
            detail: format!(
                "live and durable runtime config are not identical at rollback current {previous}"
            ),
        }
        .into());
    }

    let mut target_config = previous_config;
    set_production_pointer(&mut target_config, target, route, true);
    let prepared = prepare_pointer_activation(deps, target_config, reason, activated_by).await?;
    Ok(RollbackPointerPreflight { prepared })
}

async fn prepare_pointer_activation(
    deps: &RuntimeModelPointerSync,
    config: RuntimeConfig,
    reason: &str,
    activated_by: &str,
) -> QuantResult<PreparedProductionPointer> {
    validate_pointer_candidate(&config)
        .map_err(|detail| GovernanceError::IllegalTransition { detail })?;
    let snapshot = deps
        .runtime_config_apply
        .prepare(config.clone())
        .await
        .map_err(|error| GovernanceError::IllegalTransition {
            detail: format!("runtime config prepare failed: {error}"),
        })?;
    let version = resolve_or_create_config_version(deps, &config, reason, activated_by).await?;
    let current = deps
        .runtime_config_repo
        .load_current_activation()
        .await?
        .ok_or_else(|| GovernanceError::IllegalTransition {
            detail: "runtime config activation ledger is uninitialized".to_owned(),
        })?;
    let expected_activation_id = current.runtime_config_activation_id;
    let activation = NewRuntimeConfigActivation {
        runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
        runtime_config_version_id: version.runtime_config_version_id,
        runtime_config_approval_id: None,
        activated_by: activated_by.to_owned(),
        reason: reason.to_owned(),
        activation_kind: RuntimeConfigActivationKind::Promote,
        previous_runtime_config_version_id: Some(current.runtime_config_version_id),
        rollback_target_version_id: None,
        audit_event_id: None,
    };
    Ok(PreparedProductionPointer {
        snapshot,
        expected_activation_id,
        activation,
    })
}

async fn resolve_is_exit_scorer(
    deps: &RuntimeModelPointerSync,
    version_id: &ModelVersionId,
) -> QuantResult<bool> {
    let version = deps
        .model_registry_repo
        .find_model_version_by_id(version_id)
        .await?
        .ok_or_else(|| GovernanceError::NotFound {
            entity: "model_version",
            id: version_id.to_string(),
        })?;
    let spec = deps
        .model_registry_repo
        .find_model_spec_by_id(&version.model_spec_id)
        .await?
        .ok_or_else(|| GovernanceError::NotFound {
            entity: "model_spec",
            id: version.model_spec_id.to_string(),
        })?;
    Ok(spec.model_family.is_exit_scorer())
}

/// Clear model pointers that reference a version already retired by governance.
pub async fn sync_after_model_retire(
    deps: &RuntimeModelPointerSync,
    retired: &ModelVersionId,
    reason: &str,
    activated_by: &str,
) -> QuantResult<()> {
    let current = deps.runtime_config_apply.current();
    let retired_ref = model_version_ref(retired);
    let matches = |slot: &Option<ModelVersionRef>| {
        slot.as_ref()
            .is_some_and(|reference| reference.id == retired_ref.id)
    };
    let active_matches = matches(&current.model.active_model_version_id);
    let shadow_matches = matches(&current.model.shadow_model_version_id);
    let exit_active_matches = matches(&current.model.active_exit_model_version_id);
    let stale_categories = current
        .model
        .category_model_pointers
        .iter()
        .filter(|(_, reference)| reference.id == retired_ref.id)
        .map(|(category, _)| *category)
        .collect::<Vec<_>>();
    if !active_matches && !shadow_matches && !exit_active_matches && stale_categories.is_empty() {
        return Ok(());
    }

    let mut config = (*current).clone();
    if active_matches {
        config.model.active_model_version_id = None;
    }
    if shadow_matches {
        config.model.shadow_model_version_id = None;
    }
    if exit_active_matches {
        config.model.active_exit_model_version_id = None;
    }
    for category in &stale_categories {
        config.model.category_model_pointers.remove(category);
        tracing::warn!(
            %retired,
            ?category,
            "cleared category model pointer referencing a retired model version"
        );
    }
    persist_and_apply(deps, config, reason, activated_by).await
}

/// Arm a candidate as the shadow model pointer and promote it to `Shadow` status.
pub async fn sync_shadow_candidate(
    deps: &RuntimeModelPointerSync,
    shadow: &ModelVersionId,
    reason: &str,
    activated_by: &str,
) -> QuantResult<()> {
    ensure_shadow_armable(deps, shadow).await?;
    let is_exit = resolve_is_exit_scorer(deps, shadow).await?;
    deps.model_registry_repo
        .promote_model_to_shadow(shadow)
        .await?;

    let mut config = (*deps.runtime_config_apply.current()).clone();
    if !is_exit {
        config.model.shadow_model_version_id = Some(model_version_ref(shadow));
    }
    persist_and_apply(deps, config, reason, activated_by).await
}

async fn ensure_shadow_armable(
    deps: &RuntimeModelPointerSync,
    shadow: &ModelVersionId,
) -> QuantResult<()> {
    let version = deps
        .model_registry_repo
        .find_model_version_by_id(shadow)
        .await?
        .ok_or_else(|| GovernanceError::NotFound {
            entity: "model_version",
            id: shadow.to_string(),
        })?;
    match version.publication_status {
        PublicationStatus::Candidate | PublicationStatus::Shadow => Ok(()),
        status => Err(GovernanceError::IllegalTransition {
            detail: format!(
                "shadow model {} must be candidate or shadow (status {})",
                shadow,
                status.as_str()
            ),
        }
        .into()),
    }
}

fn config_points_to(
    config: &RuntimeConfig,
    model_version_id: &ModelVersionId,
    route: ModelPointerRoute,
) -> bool {
    let expected = model_version_id.to_string();
    let pointer = if route.is_exit {
        &config.model.active_exit_model_version_id
    } else {
        &config.model.active_model_version_id
    };
    pointer
        .as_ref()
        .is_some_and(|reference| reference.id == expected)
}

fn set_production_pointer(
    config: &mut RuntimeConfig,
    model_version_id: &ModelVersionId,
    route: ModelPointerRoute,
    clear_shadow: bool,
) {
    if route.is_exit {
        config.model.active_exit_model_version_id = Some(model_version_ref(model_version_id));
    } else {
        config.model.active_model_version_id = Some(model_version_ref(model_version_id));
        if clear_shadow {
            config.model.shadow_model_version_id = None;
        }
    }
}

fn validate_pointer_candidate(config: &RuntimeConfig) -> Result<(), String> {
    let report = validate_runtime_config(config);
    if report.has_errors() {
        return Err(format!(
            "runtime config invalid after model pointer sync: {report}"
        ));
    }
    Ok(())
}

async fn resolve_or_create_config_version(
    deps: &RuntimeModelPointerSync,
    config: &RuntimeConfig,
    reason: &str,
    activated_by: &str,
) -> QuantResult<RuntimeConfigVersionInfo> {
    let config_json = config.to_json();
    let config_hash = CanonicalDigest::content_hash_json(&config_json)?;
    match deps.runtime_config_repo.load_by_hash(&config_hash).await? {
        Some(existing) => Ok(existing),
        None => deps
            .runtime_config_repo
            .create_version(NewRuntimeConfigVersion {
                runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
                config_hash,
                schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
                config_json,
                source: RuntimeConfigVersionSource::Operator,
                created_by: activated_by.to_owned(),
                reason: reason.to_owned(),
            })
            .await
            .map_err(Into::into),
    }
}

async fn pointer_postcondition(
    deps: &RuntimeModelPointerSync,
    expected: &ModelVersionId,
    route: ModelPointerRoute,
) -> Result<bool, String> {
    let live = deps.runtime_config_apply.current();
    let live_hash = CanonicalDigest::content_hash_json(&live.to_json())
        .map_err(|error| format!("live runtime config hash failed: {error}"))?;
    let durable = deps
        .runtime_config_repo
        .load_current()
        .await
        .map_err(|error| format!("durable runtime config read failed: {error}"))?
        .ok_or_else(|| "durable runtime config is uninitialized".to_owned())?;
    let durable_config = RuntimeConfig::from_json(&durable.config_json)
        .map_err(|error| format!("durable runtime config parse failed: {error}"))?;
    let durable_hash = CanonicalDigest::content_hash_json(&durable.config_json)
        .map_err(|error| format!("durable runtime config hash failed: {error}"))?;
    Ok(config_points_to(&live, expected, route)
        && config_points_to(&durable_config, expected, route)
        && durable_hash == durable.config_hash
        && live_hash == durable_hash)
}

async fn persist_and_apply(
    deps: &RuntimeModelPointerSync,
    config: RuntimeConfig,
    reason: &str,
    activated_by: &str,
) -> QuantResult<()> {
    let report = validate_runtime_config(&config);
    if report.has_errors() {
        return Err(GovernanceError::IllegalTransition {
            detail: format!("runtime config invalid after model pointer sync: {report}"),
        }
        .into());
    }
    let prepared = deps
        .runtime_config_apply
        .prepare(config.clone())
        .await
        .map_err(|error| GovernanceError::IllegalTransition {
            detail: format!("runtime config prepare failed: {error}"),
        })?;
    let version = resolve_or_create_config_version(deps, &config, reason, activated_by).await?;
    let current = deps.runtime_config_repo.load_current_activation().await?;
    let expected_activation_id = current
        .as_ref()
        .map(|activation| activation.runtime_config_activation_id.clone());
    let previous_version_id = current
        .as_ref()
        .map(|activation| activation.runtime_config_version_id.clone());
    deps.runtime_config_repo
        .activate_version_if_current(
            expected_activation_id.as_ref(),
            NewRuntimeConfigActivation {
                runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
                runtime_config_version_id: version.runtime_config_version_id,
                runtime_config_approval_id: None,
                activated_by: activated_by.to_owned(),
                reason: reason.to_owned(),
                activation_kind: RuntimeConfigActivationKind::Promote,
                previous_runtime_config_version_id: previous_version_id,
                rollback_target_version_id: None,
                audit_event_id: None,
            },
        )
        .await?;
    prepared.publish();
    Ok(())
}

fn model_version_ref(id: &ModelVersionId) -> ModelVersionRef {
    ModelVersionRef { id: id.to_string() }
}
