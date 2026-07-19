//! Typed project/deployment lifecycle declarations.

use crate::{
    enums::runtime_config::{LifecycleBaseline, ProjectLifecycleState},
    hashing::CanonicalDigest,
    types::{BuildCommitHash, ContentHash, DeploymentEnvironment},
};
use quant_pivot_error::{
    QuantResult,
    config::ConfigError,
    config_validation::{ConfigValidationError, ConfigValidationReport},
};
use serde::{Deserialize, Serialize};

const PROJECT_LIFECYCLE_SOURCE: &str = include_str!("../../../../project-lifecycle.toml");

/// Source-controlled lifecycle baseline compiled into the application.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectLifecyclePolicy {
    pub state: ProjectLifecycleState,
    pub baseline: LifecycleBaseline,
}

impl ProjectLifecyclePolicy {
    /// Parse the lifecycle contract embedded in this build.
    pub fn compiled() -> QuantResult<Self> {
        toml::from_str(PROJECT_LIFECYCLE_SOURCE).map_err(|error| {
            ConfigError::Load(config::ConfigError::Message(error.to_string())).into()
        })
    }

    /// Canonical hash stored in the irreversible production baseline.
    pub fn content_hash(&self) -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_json(self).map_err(|error| {
            ConfigError::InvalidValue {
                field: "project-lifecycle.toml".to_owned(),
                reason: error.to_string(),
            }
            .into()
        })
    }
}

/// Environment declaration paired with the source-controlled lifecycle policy.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LifecycleDeployConfig {
    pub environment: DeploymentEnvironment,
    pub expected_state: ProjectLifecycleState,
    pub build_commit: Option<BuildCommitHash>,
    /// Hash of the deploy pipeline's successful backup/restore evidence bundle.
    pub backup_evidence_hash: Option<ContentHash>,
    /// Hash of the deterministic Config E2E/visual/accessibility evidence bundle.
    pub config_e2e_evidence_hash: Option<ContentHash>,
}

impl Default for LifecycleDeployConfig {
    fn default() -> Self {
        Self {
            environment: DeploymentEnvironment::local_development(),
            expected_state: ProjectLifecycleState::PreProductionResettable,
            build_commit: None,
            backup_evidence_hash: None,
            config_e2e_evidence_hash: None,
        }
    }
}

impl LifecycleDeployConfig {
    pub(crate) fn validate_source_contract(&self) -> QuantResult<()> {
        let source = ProjectLifecyclePolicy::compiled()?;
        if source.state != self.expected_state {
            return Err(ConfigError::from(ConfigValidationReport::single_error(
                ConfigValidationError::invalid_value(
                    "lifecycle.expected_state",
                    format!(
                        "deployment expects `{}` but project-lifecycle.toml declares `{}`",
                        self.expected_state.as_str(),
                        source.state.as_str()
                    ),
                ),
            ))
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiled_lifecycle_contract_is_typed_and_hashable() {
        let policy = ProjectLifecyclePolicy::compiled().expect("compiled policy");
        assert_eq!(policy.baseline, LifecycleBaseline::Boot);
        assert_eq!(policy.state, ProjectLifecycleState::PreProductionResettable);
        assert!(policy.content_hash().is_ok());
    }
}
