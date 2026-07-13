//! Runtime-config model pointer sync for governance publish / rollback (3.7).
//!
//! Production model switches must update **both** the registry publication state
//! and the live `model.active_model_version_id` / `shadow_model_version_id`
//! pointers so the online [`ModelRunner`] immediately scores the intended version.
//! Each switch writes a new runtime-config version + activation (WORM audit) and
//! applies through [`RuntimeConfigPort`] so subscribers (overlay applicator, etc.)
//! reload atomically with the store swap.

use std::{error::Error, fmt, sync::Arc};

use chrono::Utc;
use quant_pivot_error::{QuantError, QuantResult, governance::GovernanceError};
use quant_pivot_models::{
    domain::{
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, RuntimeConfigPort,
        RuntimeConfigVersionInfo,
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
    /// Live config apply + hot-reload propagation.
    pub runtime_config_apply: Arc<dyn RuntimeConfigPort>,
    /// Durable runtime-config version + activation ledger.
    pub runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository>,
    /// Model registry (optional shadow-arm promotion).
    pub model_registry_repo: Arc<dyn ModelRegistryRepository>,
}

/// Recoverable outcome when a rollback's registry switch committed but its
/// runtime pointer transition did not complete.
#[derive(Debug, Clone)]
pub struct RollbackPointerRecovery {
    /// Exact activation generation that must still be current when model
    /// statuses and durable config are compensated atomically.
    pub expected_runtime_config_activation_id: RuntimeConfigActivationId,
    /// Activation back to the original durable config. `None` means target
    /// activation never committed and only the generation must be verified.
    pub runtime_config_compensation: Option<NewRuntimeConfigActivation>,
    /// Original live config to reapply after the atomic durable/model commit.
    pub previous_config: RuntimeConfig,
}

#[derive(Debug)]
pub struct RollbackPointerSyncFailure {
    /// Full operator/audit context.
    pub detail: String,
    /// Atomic recovery permit. When absent, the registry must preserve the
    /// target as published because durable config ownership is unknown.
    pub recovery: Option<RollbackPointerRecovery>,
}

impl fmt::Display for RollbackPointerSyncFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl Error for RollbackPointerSyncFailure {}

#[derive(Clone, Copy)]
struct ModelPointerRoute {
    is_exit: bool,
}

/// Immutable pointer/config generation validated before model statuses change.
pub struct RollbackPointerPreflight {
    previous_config: RuntimeConfig,
    target_config: RuntimeConfig,
    previous_activation_id: RuntimeConfigActivationId,
    previous_version_id: RuntimeConfigVersionId,
    previous: ModelVersionId,
    target: ModelVersionId,
    route: ModelPointerRoute,
}

/// Switch the production active model pointer and optionally clear the shadow slot.
///
/// Routes onto the Buy-side (`active_model_version_id`) or Sell-side
/// (`active_exit_model_version_id`) pointer by the published version's model
/// family, so a Sell scorer publish never overwrites the Buy ranker pointer.
pub async fn sync_production_active(
    deps: &RuntimeModelPointerSync,
    active: &ModelVersionId,
    clear_shadow: bool,
    reason: &str,
    activated_by: &str,
) -> QuantResult<()> {
    let is_exit = resolve_is_exit_scorer(deps, active).await?;
    let mut config = (*deps.runtime_config_apply.current()).clone();
    if is_exit {
        config.model.active_exit_model_version_id = Some(model_version_ref(active));
    } else {
        config.model.active_model_version_id = Some(model_version_ref(active));
        if clear_shadow {
            config.model.shadow_model_version_id = None;
        }
    }
    persist_and_apply(deps, config, reason, activated_by).await
}

/// Validate rollback pointer ownership before model statuses change.
///
/// The later activation still compares the exact activation generation, so a
/// config transition racing after this read-only preflight fails safely.
pub async fn preflight_rollback_production_pointer(
    deps: &RuntimeModelPointerSync,
    previous: &ModelVersionId,
    target: &ModelVersionId,
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
    let previous_activation = deps
        .runtime_config_repo
        .load_current_activation()
        .await?
        .ok_or_else(|| GovernanceError::IllegalTransition {
            detail: "runtime config activation ledger is uninitialized".to_owned(),
        })?;
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

    let mut target_config = previous_config.clone();
    set_production_pointer(&mut target_config, target, route, true);
    validate_pointer_candidate(deps, &target_config)
        .map_err(|detail| GovernanceError::IllegalTransition { detail })?;
    Ok(RollbackPointerPreflight {
        previous_config,
        target_config,
        previous_activation_id: previous_activation.runtime_config_activation_id,
        previous_version_id: previous_activation.runtime_config_version_id,
        previous: previous.clone(),
        target: target.clone(),
        route,
    })
}

/// Apply a preflighted rollback pointer with durable CAS and postconditions.
///
/// A returned recovery permit must be committed in the same database
/// transaction as the model-status reversal.
pub async fn sync_rollback_production_active(
    deps: &RuntimeModelPointerSync,
    preflight: RollbackPointerPreflight,
    reason: &str,
    activated_by: &str,
) -> Result<(), RollbackPointerSyncFailure> {
    persist_and_apply_rollback(RollbackPointerTransition {
        deps,
        target_config: preflight.target_config,
        previous_config: preflight.previous_config,
        previous_activation_id: preflight.previous_activation_id,
        previous_version_id: preflight.previous_version_id,
        previous: preflight.previous,
        target: preflight.target,
        route: preflight.route,
        reason,
        activated_by,
    })
    .await
}

/// Whether the version's model family scores the Sell-side hold-vs-exit decision.
///
/// Fail-**closed**: if the version or its spec cannot be resolved we cannot know
/// which pointer to route onto, so we refuse the sync (a silent `false` would
/// misroute a Sell scorer onto the Buy `active_model_version_id`).
async fn resolve_is_exit_scorer(
    deps: &RuntimeModelPointerSync,
    version_id: &ModelVersionId,
) -> QuantResult<bool> {
    let Some(version) = deps
        .model_registry_repo
        .find_model_version_by_id(version_id)
        .await?
    else {
        return Err(GovernanceError::NotFound {
            entity: "model_version",
            id: version_id.to_string(),
        }
        .into());
    };
    let Some(spec) = deps
        .model_registry_repo
        .find_model_spec_by_id(&version.model_spec_id)
        .await?
    else {
        return Err(GovernanceError::NotFound {
            entity: "model_spec",
            id: version.model_spec_id.to_string(),
        }
        .into());
    };
    Ok(spec.model_family.is_exit_scorer())
}

/// Clear runtime-config model pointers that reference a retired version.
///
/// Covers `category_model_pointers` alongside the generic active / shadow /
/// exit-active slots (11.2.2 remediation R7): a category pointer left
/// dangling after its target retires would make category routing fail closed
/// on every inference round. The config must reflect reality the moment a
/// version retires, not eventually.
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
    let stale_categories: Vec<_> = current
        .model
        .category_model_pointers
        .iter()
        .filter(|(_, reference)| reference.id == retired_ref.id)
        .map(|(category, _)| *category)
        .collect();
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
            "cleared category_model_pointers entry referencing a retired model version"
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
    if is_exit {
        // Exit scorer shadow is governed by `opportunistic_sell.shadow_mode`, not a
        // separate runtime-config pointer.
    } else {
        config.model.shadow_model_version_id = Some(model_version_ref(shadow));
    }
    persist_and_apply(deps, config, reason, activated_by).await
}

/// Validate that a version may be wired as the shadow model in runtime config.
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

struct RollbackPointerTransition<'a> {
    deps: &'a RuntimeModelPointerSync,
    target_config: RuntimeConfig,
    previous_config: RuntimeConfig,
    previous_activation_id: RuntimeConfigActivationId,
    previous_version_id: RuntimeConfigVersionId,
    previous: ModelVersionId,
    target: ModelVersionId,
    route: ModelPointerRoute,
    reason: &'a str,
    activated_by: &'a str,
}

async fn persist_and_apply_rollback(
    transition: RollbackPointerTransition<'_>,
) -> Result<(), RollbackPointerSyncFailure> {
    let RollbackPointerTransition {
        deps,
        target_config,
        previous_config,
        previous_activation_id,
        previous_version_id,
        previous,
        target,
        route,
        reason,
        activated_by,
    } = transition;
    let original_recovery = RollbackPointerRecovery {
        expected_runtime_config_activation_id: previous_activation_id.clone(),
        runtime_config_compensation: None,
        previous_config: previous_config.clone(),
    };
    validate_pointer_candidate(deps, &target_config)
        .map_err(|detail| rollback_sync_failure(detail, Some(original_recovery.clone())))?;
    let target_version =
        resolve_or_create_config_version(deps, &target_config, reason, activated_by)
            .await
            .map_err(|error| {
                rollback_sync_failure(error.to_string(), Some(original_recovery.clone()))
            })?;
    let target_activation = deps
        .runtime_config_repo
        .activate_version_if_current(
            Some(&previous_activation_id),
            NewRuntimeConfigActivation {
                runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
                runtime_config_version_id: target_version.runtime_config_version_id.clone(),
                activated_at: Utc::now(),
                activated_by: activated_by.to_owned(),
                reason: reason.to_owned(),
                activation_kind: RuntimeConfigActivationKind::Promote,
                previous_runtime_config_version_id: Some(previous_version_id.clone()),
                rollback_target_version_id: None,
                audit_event_id: None,
            },
        )
        .await
        .map_err(|error| {
            rollback_sync_failure(error.to_string(), Some(original_recovery.clone()))
        })?;

    let apply_error = deps.runtime_config_apply.apply(target_config).await.err();
    let (target_matches, postcondition_error) =
        match pointer_postcondition(deps, &target, route).await {
            Ok(matches) => (matches, None),
            Err(error) => (false, Some(error)),
        };
    if apply_error.is_none() && target_matches {
        return Ok(());
    }

    let recovery_reason = format!(
        "compensate failed rollback pointer activation {}: {}",
        target_activation.runtime_config_activation_id,
        apply_error.as_ref().map_or_else(
            || {
                postcondition_error
                    .clone()
                    .unwrap_or_else(|| "postcondition mismatch".to_owned())
            },
            ToString::to_string,
        )
    );
    let detail = format!(
        "rollback pointer transition from {previous} to {target} failed; apply_error={apply_error:?}; target_postcondition={target_matches}; postcondition_error={postcondition_error:?}"
    );
    Err(rollback_sync_failure(
        detail,
        Some(RollbackPointerRecovery {
            expected_runtime_config_activation_id: target_activation.runtime_config_activation_id,
            runtime_config_compensation: Some(NewRuntimeConfigActivation {
                runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
                runtime_config_version_id: previous_version_id.clone(),
                activated_at: Utc::now(),
                activated_by: activated_by.to_owned(),
                reason: recovery_reason,
                activation_kind: RuntimeConfigActivationKind::Rollback,
                previous_runtime_config_version_id: Some(
                    target_activation.runtime_config_version_id,
                ),
                rollback_target_version_id: Some(previous_version_id),
                audit_event_id: None,
            }),
            previous_config,
        }),
    ))
}

const fn rollback_sync_failure(
    detail: String,
    recovery: Option<RollbackPointerRecovery>,
) -> RollbackPointerSyncFailure {
    RollbackPointerSyncFailure { detail, recovery }
}

/// Reapply the original live config after atomic durable compensation.
///
/// Verifies the complete live/durable config hash and routed pointer after the
/// repository reverses model statuses and config activation together.
pub async fn finalize_rollback_pointer_recovery(
    deps: &RuntimeModelPointerSync,
    previous: &ModelVersionId,
    recovery: &RollbackPointerRecovery,
) -> Result<(), String> {
    let route = ModelPointerRoute {
        is_exit: resolve_is_exit_scorer(deps, previous)
            .await
            .map_err(|error| error.to_string())?,
    };
    let apply_error = deps
        .runtime_config_apply
        .apply(recovery.previous_config.clone())
        .await
        .err();
    match pointer_postcondition(deps, previous, route).await {
        Ok(true) => {
            if let Some(error) = apply_error {
                tracing::warn!(
                    %error,
                    %previous,
                    "rollback recovery apply returned an error but exact live/durable postcondition is restored"
                );
            }
            Ok(())
        }
        Ok(false) => Err(format!(
            "rollback recovery postcondition does not point live+durable config to {previous}; apply_error={apply_error:?}"
        )),
        Err(error) => Err(format!(
            "rollback recovery postcondition read failed: {error}; apply_error={apply_error:?}"
        )),
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

fn validate_pointer_candidate(
    deps: &RuntimeModelPointerSync,
    config: &RuntimeConfig,
) -> Result<(), String> {
    let report = validate_runtime_config(config);
    if report.has_errors() {
        return Err(format!(
            "runtime config invalid after model pointer sync: {report}"
        ));
    }
    deps.runtime_config_apply
        .preflight(config)
        .map_err(|error| format!("runtime config preflight failed: {error}"))
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

/// Write a new runtime-config version + activation and apply to the live system.
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

    deps.runtime_config_apply
        .preflight(&config)
        .map_err(|error| GovernanceError::IllegalTransition {
            detail: format!("runtime config preflight failed: {error}"),
        })?;

    let config_json = config.to_json();
    let config_hash = CanonicalDigest::content_hash_json(&config_json)?;
    let version = match deps.runtime_config_repo.load_by_hash(&config_hash).await? {
        Some(existing) => existing,
        None => {
            deps.runtime_config_repo
                .create_version(NewRuntimeConfigVersion {
                    runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
                    config_hash,
                    schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
                    config_json,
                    source: RuntimeConfigVersionSource::Operator,
                    created_by: activated_by.to_owned(),
                    reason: reason.to_owned(),
                })
                .await?
        }
    };

    let previous = deps
        .runtime_config_repo
        .load_current()
        .await?
        .map(|row| row.runtime_config_version_id);

    deps.runtime_config_repo
        .activate_version(NewRuntimeConfigActivation {
            runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
            runtime_config_version_id: version.runtime_config_version_id,
            activated_at: Utc::now(),
            activated_by: activated_by.to_owned(),
            reason: reason.to_owned(),
            activation_kind: RuntimeConfigActivationKind::Promote,
            previous_runtime_config_version_id: previous,
            rollback_target_version_id: None,
            audit_event_id: None,
        })
        .await?;

    deps.runtime_config_apply
        .apply(config)
        .await
        .map_err(|error| {
            QuantError::from(GovernanceError::IllegalTransition {
                detail: format!("runtime config apply failed after activation: {error}"),
            })
        })
}

fn model_version_ref(id: &ModelVersionId) -> ModelVersionRef {
    ModelVersionRef { id: id.to_string() }
}
