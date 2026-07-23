//! Process deployment environment declaration.

use serde::Deserialize;

use crate::types::DeploymentEnvironment;

/// Environment identity used only for environment-specific operational safety.
///
/// It is intentionally not a release, schema, build, or policy authority.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeploymentConfig {
    pub environment: DeploymentEnvironment,
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        Self {
            environment: DeploymentEnvironment::local_development(),
        }
    }
}

impl DeploymentConfig {
    /// Production deployments may never execute the destructive fresh-boot reset command.
    #[must_use]
    pub fn permits_destructive_reset(&self) -> bool {
        self.environment.as_str() != "production"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_production_environment_blocks_destructive_reset() {
        let production = DeploymentConfig {
            environment: DeploymentEnvironment::parse("production").expect("production"),
        };
        assert!(!production.permits_destructive_reset());
        assert!(DeploymentConfig::default().permits_destructive_reset());
    }
}
