//! Deploy configuration tree (`config/oxide-arb.toml`, restart to apply).
//!
//! [`DeployConfig`] owns everything that is structurally bound to a process
//! start: connection endpoints and pools, channel capacities and shard counts,
//! credential sources, the web server, and logging. Operator tunables that must
//! change **without** a restart live in the versioned
//! [`RuntimeConfig`](crate::runtime_config::RuntimeConfig) instead and are
//! managed through the governed runtime-config API — never by editing TOML.
//!
//! # Loading precedence (high → low)
//!
//! 1. Environment variables (`OXIDE_ARB__*`)
//! 2. `config/oxide-arb.toml`
//! 3. Hard-coded defaults
//!
//! Every section rejects unknown keys (`deny_unknown_fields`): a typo or a
//! leftover runtime section in the TOML aborts startup instead of being
//! silently ignored.

mod cache;
mod db;
mod execution;
mod fees;
mod keys;
mod market_data;
mod observability;
mod polymarket;
mod settlement;
pub mod validation;
mod web;

pub use cache::*;
pub use db::*;
pub use execution::*;
pub use fees::*;
pub use keys::*;
pub use market_data::*;
pub use observability::*;
pub use polymarket::*;
pub use settlement::*;
pub use web::*;

use crate::{
    config::validation::{validate_deploy_common, validate_deploy_for_mode},
    enums::common::ExecutionMode,
};
use oxide_arb_error::{
    OxideResult, config::ConfigError, config_validation::ConfigValidationReport,
};
use serde::Deserialize;

/// Deserialized deploy-configuration root.
///
/// Each section maps 1:1 to a `[section]` in `oxide-arb.toml`. Wrap in an
/// `Arc` for sharing across async tasks — the struct itself is plain data.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeployConfig {
    /// Polymarket platform endpoints, chain, and fee schedule.
    pub polymarket: PolymarketConfig,
    /// Market-data connections (CLOB WebSocket + Gamma catalog).
    pub market_data: MarketDataDeployConfig,
    /// Logging (level + format).
    pub observability: ObservabilityConfig,
    /// Postgres + `ClickHouse` connections and write batching.
    pub db: DatabaseConfig,
    /// Redis + in-process cache layer.
    pub cache: CacheConfig,
    /// Credential source (env or keystore).
    pub keys: KeysConfig,
    /// HTTP/WebSocket server + JWT.
    pub web: WebConfig,
    /// Execution structural parameters (book-apply sharding).
    pub execution: ExecutionDeployConfig,
    /// Settlement structural parameters (channel capacity).
    pub settlement: SettlementDeployConfig,
}

impl DeployConfig {
    /// Load configuration from the given directory.
    ///
    /// Loads `{dir}/oxide-arb.toml` (optional — defaults apply when absent)
    /// and merges `OXIDE_ARB__*` environment variables on top. Runs the
    /// mode-agnostic semantic validation before returning — fatal errors abort
    /// startup, warnings are logged via `tracing`.
    pub fn load(config_dir: &str) -> OxideResult<Self> {
        let builder = config::Config::builder()
            .add_source(config::File::with_name(&format!("{config_dir}/oxide-arb")).required(false))
            .add_source(
                config::Environment::with_prefix("OXIDE_ARB")
                    .prefix_separator("__")
                    .separator("__")
                    .try_parsing(true),
            );

        let deploy: Self = builder
            .build()
            .map_err(ConfigError::Load)?
            .try_deserialize()
            .map_err(ConfigError::Load)?;

        deploy.ensure_valid_common()?;
        Ok(deploy)
    }

    /// Run the mode-agnostic validation and fail closed on errors.
    ///
    /// Warnings are streamed to `tracing::warn` as a side effect so callers
    /// get uniform telemetry. Also used by [`Self::load`].
    pub fn ensure_valid_common(&self) -> OxideResult<()> {
        let report = validate_deploy_common(self);
        for w in &report.warnings {
            tracing::warn!("Deploy config warning: {w}");
        }
        if report.has_errors() {
            return Err(ConfigError::from(report).into());
        }
        Ok(())
    }

    /// Run the **mode-aware** portion of deploy validation (credential policy,
    /// JWT strength). Called once the effective [`ExecutionMode`] is known —
    /// the persisted operational mode, not a config value.
    #[must_use]
    pub fn validate_for_mode(&self, mode: ExecutionMode) -> ConfigValidationReport {
        let report = validate_deploy_for_mode(self, mode);
        for w in &report.warnings {
            tracing::warn!(mode = ?mode, "Deploy config warning: {w}");
        }
        report
    }

    /// Fail-closed gate for mode-aware validation.
    ///
    /// Live/Paper credential policy and JWT strength must pass before
    /// subsystems connect to PG/CLOB. Warnings are logged; only errors abort.
    pub fn ensure_valid_for_mode(&self, mode: ExecutionMode) -> OxideResult<()> {
        let report = self.validate_for_mode(mode);
        if report.has_errors() {
            return Err(ConfigError::from(report).into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{env::var, path::Path};

    #[test]
    fn default_config_loads_when_file_absent() {
        let deploy = DeployConfig::load("nonexistent_dir_for_test");
        assert!(deploy.is_ok(), "defaults should load: {deploy:?}");
    }

    #[test]
    fn defaults_validate_clean() {
        DeployConfig::default()
            .ensure_valid_common()
            .expect("defaults must validate");
    }

    #[test]
    fn validate_for_mode_dry_run_permissive() {
        let deploy = DeployConfig::default();
        assert!(!deploy.validate_for_mode(ExecutionMode::DryRun).has_errors());
    }

    #[test]
    fn ensure_valid_for_mode_fails_on_live_missing_credentials() {
        let deploy = DeployConfig::default();
        let err = deploy
            .ensure_valid_for_mode(ExecutionMode::Live)
            .expect_err("Live without credentials must fail closed");
        assert!(err.to_string().contains("missing required credentials"));
    }

    #[test]
    fn unknown_section_is_rejected() {
        let toml = "[treasury]\ntarget_balance_usd = \"1000\"\n";
        let result: Result<DeployConfig, _> = toml::from_str(toml);
        assert!(result.is_err(), "stale [treasury] section must be fatal");
    }

    #[test]
    fn runtime_sections_are_rejected_in_deploy_toml() {
        for section in [
            "detection",
            "risk",
            "notification",
            "analytics",
            "sizing",
            "treasury",
        ] {
            let toml = format!("[{section}]\n");
            let result: Result<DeployConfig, _> = toml::from_str(&toml);
            assert!(result.is_err(), "runtime section [{section}] must be fatal");
        }
    }

    /// Resolve the workspace `config/` directory from the crate manifest.
    fn workspace_config_dir() -> std::path::PathBuf {
        let crate_dir = var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_owned());
        Path::new(&crate_dir)
            .ancestors()
            .nth(2)
            .expect("workspace root")
            .join("config")
    }

    #[test]
    fn shipped_toml_template_deserializes() {
        let config_dir = workspace_config_dir();
        if !config_dir.join("oxide-arb.toml").exists() {
            eprintln!("skipping shipped_toml_template_deserializes: template missing");
            return;
        }
        let dir_str = config_dir.to_str().expect("utf-8");
        let deploy = DeployConfig::load(dir_str);
        assert!(
            deploy.is_ok(),
            "config/oxide-arb.toml failed to deserialize: {deploy:?}"
        );
    }

    /// The dev template documents itself as "all values shown are the
    /// compiled-in defaults" — hold it to that, so the TOML and the Rust
    /// `Default` impls can never drift apart silently.
    #[test]
    fn shipped_toml_template_matches_rust_defaults() {
        let template = workspace_config_dir().join("oxide-arb.toml");
        if !template.exists() {
            eprintln!("skipping shipped_toml_template_matches_rust_defaults: template missing");
            return;
        }
        // Parse the file directly (no env overlay) so a developer's
        // OXIDE_ARB__* variables cannot affect the comparison.
        let raw = std::fs::read_to_string(&template).expect("read dev template");
        let parsed: DeployConfig = toml::from_str(&raw).expect("dev template deserializes");
        assert_eq!(
            parsed,
            DeployConfig::default(),
            "config/oxide-arb.toml drifted from the compiled-in defaults"
        );
    }

    /// Build a `DeployConfig` from an injected `OXIDE_ARB__*` map, exactly as
    /// [`DeployConfig::load`] would merge the real process environment —
    /// without mutating process-global state (parallel-test safe).
    fn load_with_env(env: &[(&str, &str)]) -> Result<DeployConfig, config::ConfigError> {
        let source = config::Environment::with_prefix("OXIDE_ARB")
            .prefix_separator("__")
            .separator("__")
            .try_parsing(true)
            .source(Some(
                env.iter()
                    .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                    .collect(),
            ));
        config::Config::builder()
            .add_source(source)
            .build()?
            .try_deserialize()
    }

    #[test]
    fn env_overlay_overrides_defaults() {
        let deploy = load_with_env(&[
            ("OXIDE_ARB__DB__POSTGRES__HOST", "db.internal"),
            ("OXIDE_ARB__DB__POSTGRES__PORT", "6432"),
            ("OXIDE_ARB__OBSERVABILITY__LOG_JSON", "true"),
        ])
        .expect("env overlay must deserialize");
        assert_eq!(deploy.db.postgres.host, "db.internal");
        assert_eq!(deploy.db.postgres.port, 6432);
        assert!(deploy.observability.log_json);
        // Untouched keys keep their defaults.
        assert_eq!(
            deploy.db.postgres.database,
            DeployConfig::default().db.postgres.database
        );
    }

    #[test]
    fn unknown_env_key_is_rejected_by_deny_unknown_fields() {
        let result = load_with_env(&[("OXIDE_ARB__DB__POSTGRES__HOSTNAME_TYPO", "oops")]);
        assert!(result.is_err(), "typo'd env key must abort startup");

        let result = load_with_env(&[("OXIDE_ARB__TREASURY__TARGET_BALANCE_USD", "1000")]);
        assert!(
            result.is_err(),
            "stale [treasury] env key must abort startup"
        );
    }

    #[test]
    fn production_example_toml_deserializes_and_validates() {
        let template = workspace_config_dir().join("oxide-arb.production.example.toml");
        if !template.exists() {
            eprintln!("skipping production_example_toml_deserializes: {template:?} missing");
            return;
        }
        let raw = std::fs::read_to_string(&template).expect("read production example");
        let parsed: DeployConfig =
            toml::from_str(&raw).expect("production example must deserialize");
        parsed
            .ensure_valid_common()
            .expect("production example must pass mode-agnostic validation");
    }
}
