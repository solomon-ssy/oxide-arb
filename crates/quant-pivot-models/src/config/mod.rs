//! Deploy configuration tree (`config/quant-pivot.toml`, restart to apply).
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
//! The `config` crate merges sources in registration order; **later wins**:
//!
//! 1. Environment variables (`QUANT_PIVOT__*`)
//! 2. `config/quant-pivot.local.toml` (optional, gitignored — for local secrets)
//! 3. `config/quant-pivot.toml`
//! 4. Hard-coded defaults (`serde` `default` on each struct field)
//!
//! Every section rejects unknown keys (`deny_unknown_fields`): a typo or a
//! leftover runtime section in the TOML aborts startup instead of being
//! silently ignored.

mod cache;
mod db;
mod fees;
mod keys;
mod market_data;
mod observability;
mod polymarket;
mod quant;
mod research;
pub mod validation;
mod web;

use std::collections::HashMap;

pub use cache::*;
pub use db::*;
pub use fees::*;
pub use keys::*;
pub use market_data::*;
pub use observability::*;
pub use polymarket::*;
pub use quant::*;
pub use research::*;
pub use web::*;

use crate::{
    config::validation::{validate_deploy_common, validate_deploy_for_quant_mode},
    enums::quant::QuantRuntimeMode,
};
use quant_pivot_error::{
    QuantResult, config::ConfigError, config_validation::ConfigValidationReport,
};
use serde::Deserialize;

/// Deserialized deploy-configuration root.
///
/// Each section maps 1:1 to a `[section]` in `quant-pivot.toml`. Wrap in an
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
    /// Quant pivot structural parameters (workers, credential load policy).
    pub quant: QuantDeployConfig,
    /// Research plane settings (artifact-store root).
    pub research: ResearchDeployConfig,
}

impl DeployConfig {
    /// Load configuration from the given directory.
    ///
    /// Loads `{dir}/quant-pivot.toml` and optional `{dir}/quant-pivot.local.toml`,
    /// then merges `QUANT_PIVOT__*` environment variables (env overrides file).
    /// Runs mode-agnostic semantic validation before returning.
    pub fn load(config_dir: &str) -> QuantResult<Self> {
        let mut deploy: Self = build_config(config_dir, None)
            .map_err(ConfigError::Load)?
            .try_deserialize()
            .map_err(ConfigError::Load)?;
        deploy.keys.normalize();
        deploy.ensure_valid_common()?;
        Ok(deploy)
    }
}

/// Shared config-crate builder: file → local overlay → environment.
fn build_config(
    config_dir: &str,
    env: Option<HashMap<String, String>>,
) -> Result<config::Config, config::ConfigError> {
    let mut builder = config::Config::builder()
        .add_source(config::File::with_name(&format!("{config_dir}/quant-pivot")).required(false))
        .add_source(
            config::File::with_name(&format!("{config_dir}/quant-pivot.local")).required(false),
        );

    let mut env_source = config::Environment::with_prefix("QUANT_PIVOT")
        .prefix_separator("__")
        .separator("__")
        .try_parsing(true);

    if let Some(map) = env {
        env_source = env_source.source(Some(map));
    }

    builder = builder.add_source(env_source);
    builder.build()
}

impl DeployConfig {
    /// Run the mode-agnostic validation and fail closed on errors.
    ///
    /// Warnings are streamed to `tracing::warn` as a side effect so callers
    /// get uniform telemetry. Also used by [`Self::load`].
    pub fn ensure_valid_common(&self) -> QuantResult<()> {
        let report = validate_deploy_common(self);
        for w in &report.warnings {
            tracing::warn!("Deploy config warning: {w}");
        }
        if report.has_errors() {
            return Err(ConfigError::from(report).into());
        }
        Ok(())
    }

    /// Run the **quant-mode-aware** portion of deploy validation (credential
    /// policy, JWT strength). Called once the effective [`QuantRuntimeMode`]
    /// is known — the persisted operational mode, not a config value.
    #[must_use]
    pub fn validate_for_quant_mode(&self, mode: QuantRuntimeMode) -> ConfigValidationReport {
        let report = validate_deploy_for_quant_mode(self, mode);
        for w in &report.warnings {
            tracing::warn!(mode = ?mode, "Deploy config warning: {w}");
        }
        report
    }

    /// Fail-closed gate for quant-mode-aware validation.
    pub fn ensure_valid_for_quant_mode(&self, mode: QuantRuntimeMode) -> QuantResult<()> {
        let report = self.validate_for_quant_mode(mode);
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
    fn validate_for_quant_mode_report_only_permissive() {
        let deploy = DeployConfig::default();
        assert!(
            !deploy
                .validate_for_quant_mode(QuantRuntimeMode::ReportOnly)
                .has_errors()
        );
    }

    #[test]
    fn ensure_valid_for_quant_mode_fails_on_auto_execution_missing_credentials() {
        let deploy = DeployConfig::default();
        let err = deploy
            .ensure_valid_for_quant_mode(QuantRuntimeMode::AutoExecution)
            .expect_err("AutoExecution without credentials must fail closed");
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
        if !config_dir.join("quant-pivot.toml").exists() {
            eprintln!("skipping shipped_toml_template_deserializes: template missing");
            return;
        }
        let dir_str = config_dir.to_str().expect("utf-8");
        let deploy = DeployConfig::load(dir_str);
        assert!(
            deploy.is_ok(),
            "config/quant-pivot.toml failed to deserialize: {deploy:?}"
        );
    }

    /// Build a `DeployConfig` from an injected `QUANT_PIVOT__*` map, exactly as
    /// [`DeployConfig::load`] would merge the real process environment —
    /// without mutating process-global state (parallel-test safe).
    fn load_with_env(env: &[(&str, &str)]) -> Result<DeployConfig, config::ConfigError> {
        let map = env
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        let mut deploy: DeployConfig =
            build_config("nonexistent_dir_for_test", Some(map))?.try_deserialize()?;
        deploy.keys.normalize();
        Ok(deploy)
    }

    #[test]
    fn keys_env_overrides_toml_file() {
        let dir = std::env::temp_dir().join(format!("quant_pivot_cfg_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp config dir");

        let toml = r#"
[keys]
source = "env"
private_key = "0xfrom_toml"
"#;
        std::fs::write(dir.join("quant-pivot.toml"), toml).expect("write toml");

        let dir_str = dir.to_str().expect("utf-8");
        let from_file = DeployConfig::load(dir_str).expect("load from toml");
        assert_eq!(from_file.keys.private_key.as_deref(), Some("0xfrom_toml"));

        let map = std::iter::once((
            "QUANT_PIVOT__KEYS__PRIVATE_KEY".to_owned(),
            "0xfrom_env".to_owned(),
        ))
        .collect();
        let mut from_env: DeployConfig = build_config(dir_str, Some(map))
            .expect("build")
            .try_deserialize()
            .expect("deserialize");
        from_env.keys.normalize();
        assert_eq!(from_env.keys.private_key.as_deref(), Some("0xfrom_env"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn keys_local_toml_overrides_base_toml() {
        let dir =
            std::env::temp_dir().join(format!("quant_pivot_local_cfg_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp config dir");

        std::fs::write(
            dir.join("quant-pivot.toml"),
            r#"
[keys]
private_key = "0xbase"
"#,
        )
        .expect("write base");
        std::fs::write(
            dir.join("quant-pivot.local.toml"),
            r#"
[keys]
private_key = "0xlocal"
"#,
        )
        .expect("write local");

        let deploy = DeployConfig::load(dir.to_str().expect("utf-8")).expect("load");
        assert_eq!(deploy.keys.private_key.as_deref(), Some("0xlocal"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn env_overlay_overrides_defaults() {
        let deploy = load_with_env(&[
            ("QUANT_PIVOT__DB__POSTGRES__HOST", "db.internal"),
            ("QUANT_PIVOT__DB__POSTGRES__PORT", "6432"),
            ("QUANT_PIVOT__OBSERVABILITY__LOG_JSON", "true"),
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
        let result = load_with_env(&[("QUANT_PIVOT__DB__POSTGRES__HOSTNAME_TYPO", "oops")]);
        assert!(result.is_err(), "typo'd env key must abort startup");

        let result = load_with_env(&[("QUANT_PIVOT__TREASURY__TARGET_BALANCE_USD", "1000")]);
        assert!(
            result.is_err(),
            "stale [treasury] env key must abort startup"
        );
    }

    #[test]
    fn production_example_toml_deserializes_and_validates() {
        let template = workspace_config_dir().join("quant-pivot.production.example.toml");
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
