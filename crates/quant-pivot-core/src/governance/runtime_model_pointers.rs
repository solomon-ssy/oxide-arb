//! Runtime-config model pointer sync for governance publish / rollback (3.7).
//!
//! Production model switches must update **both** the registry publication state
//! and the live `model.active_model_version_id` / `shadow_model_version_id`
//! pointers so the online [`ModelRunner`] immediately scores the intended version.
//! Each switch writes a new runtime-config version + activation (WORM audit) and
//! applies through [`RuntimeConfigPort`] so subscribers (overlay applicator, etc.)
//! reload atomically with the store swap.

use std::sync::Arc;

use chrono::Utc;
use quant_pivot_error::{QuantError, QuantResult, governance::GovernanceError};
use quant_pivot_models::{
    domain::{NewRuntimeConfigActivation, NewRuntimeConfigVersion, RuntimeConfigPort},
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

/// Switch the production active model pointer and optionally clear the shadow slot.
pub async fn sync_production_active(
    deps: &RuntimeModelPointerSync,
    active: &ModelVersionId,
    clear_shadow: bool,
    reason: &str,
    activated_by: &str,
) -> QuantResult<()> {
    let mut config = (*deps.runtime_config_apply.current()).clone();
    config.model.active_model_version_id = Some(model_version_ref(active));
    if clear_shadow {
        config.model.shadow_model_version_id = None;
    }
    persist_and_apply(deps, config, reason, activated_by).await
}

/// Clear runtime-config model pointers that reference a retired version.
pub async fn sync_after_model_retire(
    deps: &RuntimeModelPointerSync,
    retired: &ModelVersionId,
    reason: &str,
    activated_by: &str,
) -> QuantResult<()> {
    let current = deps.runtime_config_apply.current();
    let retired_ref = model_version_ref(retired);
    let active_matches = current
        .model
        .active_model_version_id
        .as_ref()
        .is_some_and(|reference| reference.id == retired_ref.id);
    let shadow_matches = current
        .model
        .shadow_model_version_id
        .as_ref()
        .is_some_and(|reference| reference.id == retired_ref.id);
    if !active_matches && !shadow_matches {
        return Ok(());
    }
    let mut config = (*current).clone();
    if active_matches {
        config.model.active_model_version_id = None;
    }
    if shadow_matches {
        config.model.shadow_model_version_id = None;
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
    deps.model_registry_repo
        .promote_model_to_shadow(shadow)
        .await?;

    let mut config = (*deps.runtime_config_apply.current()).clone();
    config.model.shadow_model_version_id = Some(model_version_ref(shadow));
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
