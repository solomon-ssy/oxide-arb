//! Startup verification for the irreversible project production lifecycle.

use crate::app::InfraBundle;
use quant_pivot_error::{QuantResult, storage::StorageError};
use quant_pivot_models::{
    config::{CompiledBuildIdentity, DeployConfig, ProjectLifecyclePolicy},
    enums::runtime_config::ProjectLifecycleState,
    hashing::CanonicalDigest,
    runtime_config::DecisionPolicySnapshot,
};
use quant_pivot_repository::traits::PolicyRepository;
use std::fmt::Display;

/// Fail closed unless source, deployment, database and active policy bundle agree.
pub async fn verify_project_lifecycle(
    deploy: &DeployConfig,
    infra: &InfraBundle,
    policies: &dyn PolicyRepository,
    active_policy: &DecisionPolicySnapshot,
) -> QuantResult<()> {
    let source = ProjectLifecyclePolicy::compiled()?;
    let baseline = policies.load_production_baseline().await?;

    match baseline {
        None => {
            if source.state != ProjectLifecycleState::PreProductionResettable
                || deploy.lifecycle.expected_state != ProjectLifecycleState::PreProductionResettable
            {
                return Err(lifecycle_conflict(
                    "production_frozen is declared but the immutable database baseline is absent",
                )
                .into());
            }
        }
        Some(baseline) => {
            let build_identity = CompiledBuildIdentity::compiled()?;
            if source.state != ProjectLifecycleState::ProductionFrozen
                || deploy.lifecycle.expected_state != ProjectLifecycleState::ProductionFrozen
            {
                return Err(lifecycle_conflict(
                    "database is production-frozen; update project-lifecycle.toml and the deployment expectation before restarting",
                )
                .into());
            }
            if baseline.environment != deploy.lifecycle.environment {
                return Err(lifecycle_conflict(
                    "sealed environment differs from the deployment environment",
                )
                .into());
            }
            if !build_identity.clean {
                return Err(lifecycle_conflict(
                    "production-frozen startup requires a clean compiled Git identity",
                )
                .into());
            }
            if build_identity.build_commit != baseline.build_commit {
                return Err(lifecycle_conflict(
                    "sealed build commit differs from the deployment artifact",
                )
                .into());
            }
            if baseline.postgres_schema_fingerprint != infra.postgres_schema_fingerprint
                || baseline.clickhouse_schema_fingerprint != infra.clickhouse_schema_fingerprint
            {
                return Err(lifecycle_conflict(
                    "sealed database schema fingerprints differ from the verified runtime schema",
                )
                .into());
            }
            let active_policy_bundle_hash = CanonicalDigest::content_hash_json(active_policy)
                .map_err(|error| lifecycle_conflict(error.to_string()))?;
            if baseline.policy_bundle_hash != active_policy_bundle_hash {
                return Err(lifecycle_conflict(
                    "sealed policy bundle differs from the active decision policy snapshot",
                )
                .into());
            }
            if baseline.lifecycle_policy_hash != source.content_hash()? {
                return Err(lifecycle_conflict(
                    "sealed lifecycle policy hash differs from project-lifecycle.toml",
                )
                .into());
            }
        }
    }
    Ok(())
}

fn lifecycle_conflict(detail: impl Display) -> StorageError {
    StorageError::state_conflict("system_production_baseline", Option::<&str>::None, detail)
}
