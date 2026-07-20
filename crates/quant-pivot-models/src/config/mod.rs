//! Deploy configuration tree (`config/quant-pivot.toml`, restart to apply).
//!
//! [`DeployConfig`] owns everything that is structurally bound to a process
//! start: connection endpoints and pools, channel capacities and shard counts,
//! credential sources, the web server, and logging. Operator tunables that must
//! change **without** a restart live in independently revisioned
//! [`DecisionPolicySnapshot`](crate::runtime_config::DecisionPolicySnapshot) instead and are
//! managed through the governed Config API — never by editing TOML.
//!
//! # Loading precedence (high → low)
//!
//! The `config` crate merges sources in registration order; **later wins**:
//!
//! 1. Environment-specific immutable non-secret TOML override
//! 2. `config/quant-pivot.toml`
//! 3. Hard-coded defaults (`serde` `default` on each struct field)
//! 4. systemd Credentials resolved from typed references at bootstrap
//!
//! Every section rejects unknown keys (`deny_unknown_fields`): a typo or a
//! leftover runtime section in the TOML aborts startup instead of being
//! silently ignored.

mod cache;
mod db;
mod domain_sources;
mod keys;
mod lifecycle;
mod market_data;
mod observability;
mod polymarket;
mod quant;
mod research;
pub mod secret;
pub mod validation;
mod web;

pub use cache::*;
pub use db::*;
pub use domain_sources::*;
pub use keys::*;
pub use lifecycle::*;
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
    QuantResult,
    config::ConfigError,
    config_validation::{ConfigValidationError, ConfigValidationReport},
};
use serde::Deserialize;

/// Deserialized deploy-configuration root.
///
/// Each section maps 1:1 to a `[section]` in `quant-pivot.toml`. Wrap in an
/// `Arc` for sharing across async tasks — the struct itself is plain data.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeployConfig {
    /// Environment identity and irreversible project-lifecycle expectation.
    pub lifecycle: LifecycleDeployConfig,
    /// Polymarket platform endpoints, chain, and fee schedule.
    pub polymarket: PolymarketConfig,
    /// Market-data connections (CLOB WebSocket + Gamma catalog).
    pub market_data: MarketDataDeployConfig,
    /// External-vertical domain data sources (Binance klines + Chainlink).
    pub domain_sources: DomainSourcesConfig,
    /// Logging (level + format).
    pub observability: ObservabilityConfig,
    /// Telegram/webhook bindings and systemd credential references.
    pub notifications: NotificationChannelsConfig,
    /// Postgres + `ClickHouse` connections and write batching.
    pub db: DatabaseConfig,
    /// Redis + in-process cache layer.
    pub cache: CacheConfig,
    /// Wallet credential reference.
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
    /// Loads `{dir}/quant-pivot.toml` and an optional immutable environment
    /// override, resolves typed systemd credential references, and runs
    /// mode-agnostic semantic validation before returning.
    pub fn load(config_dir: &str) -> QuantResult<Self> {
        let mut deploy = Self::load_internal(config_dir, false)?;
        let has_migration_password = deploy.db.postgres.migration.password.is_some()
            || deploy.db.clickhouse.migration.password.is_some();
        let production_profile = deploy.db.clickhouse.deployment_id != "local-development"
            || deploy.research.artifact_store.kind == ArtifactStoreKind::S3;
        if production_profile && has_migration_password {
            return Err(ConfigError::from(ConfigValidationReport::single_error(
                ConfigValidationError::invalid_value(
                    "db.*.migration.password",
                    "production runtime configuration must not contain DDL credentials",
                ),
            ))
            .into());
        }
        deploy.db.postgres.migration.password = None;
        deploy.db.clickhouse.migration.password = None;
        Ok(deploy)
    }

    /// Load deploy configuration for schema plan/apply/manifest commands.
    ///
    /// Unlike the runtime projection, this retains migration-only passwords.
    pub fn load_for_migration(config_dir: &str) -> QuantResult<Self> {
        Self::load_internal(config_dir, true)
    }

    fn load_internal(config_dir: &str, include_migration_credentials: bool) -> QuantResult<Self> {
        let mut deploy: Self = build_config(config_dir)
            .map_err(ConfigError::Load)?
            .try_deserialize()
            .map_err(ConfigError::Load)?;
        deploy.keys.normalize();
        deploy.polymarket.relayer.normalize();
        deploy
            .domain_sources
            .chainlink_data_streams
            .normalize_credentials();
        normalize_migration_password(&mut deploy.db.postgres.migration.password);
        normalize_migration_password(&mut deploy.db.clickhouse.migration.password);
        deploy.resolve_runtime_credentials()?;
        if include_migration_credentials {
            deploy.resolve_migration_credentials()?;
        }
        deploy.ensure_valid_common()?;
        deploy.lifecycle.validate_source_contract()?;
        Ok(deploy)
    }
}

fn normalize_migration_password(password: &mut Option<secret::SystemdCredentialRef>) {
    if password
        .as_ref()
        .is_some_and(|credential| !credential.is_configured())
    {
        *password = None;
    }
}

impl DeployConfig {
    fn resolve_runtime_credentials(&mut self) -> QuantResult<()> {
        self.polymarket.onchain.resolve_credentials()?;
        if let Some(private_key) = self.keys.private_key.as_mut() {
            private_key.resolve("keys.private_key")?;
        }
        self.db.postgres.password.resolve("db.postgres.password")?;
        self.db
            .clickhouse
            .password
            .resolve("db.clickhouse.password")?;
        self.cache.redis.password.resolve("cache.redis.password")?;
        self.web.jwt.signing_key.resolve("web.jwt.signing_key")?;
        self.research
            .evidence_attestation
            .signing_key
            .resolve("research.evidence_attestation.signing_key")?;
        for (index, credential) in self
            .research
            .evidence_attestation
            .previous_signing_keys
            .iter_mut()
            .enumerate()
        {
            credential.resolve(&format!(
                "research.evidence_attestation.previous_signing_keys[{index}]"
            ))?;
        }
        if let Some(api_key) = self.polymarket.relayer.api_key.as_mut() {
            api_key.resolve("polymarket.relayer.api_key")?;
        }
        if let Some(api_key) = self.domain_sources.chainlink_data_streams.api_key.as_mut() {
            api_key.resolve("domain_sources.chainlink_data_streams.api_key")?;
        }
        if let Some(api_secret) = self
            .domain_sources
            .chainlink_data_streams
            .api_secret
            .as_mut()
        {
            api_secret.resolve("domain_sources.chainlink_data_streams.api_secret")?;
        }
        self.notifications
            .telegram
            .bot_token_credential
            .resolve("notifications.telegram.bot_token_credential")?;
        self.notifications
            .webhook
            .authorization_credential
            .resolve("notifications.webhook.authorization_credential")?;
        Ok(())
    }

    fn resolve_migration_credentials(&mut self) -> QuantResult<()> {
        if let Some(password) = self.db.postgres.migration.password.as_mut() {
            password.resolve("db.postgres.migration.password")?;
        }
        if let Some(password) = self.db.clickhouse.migration.password.as_mut() {
            password.resolve("db.clickhouse.migration.password")?;
        }
        Ok(())
    }
}

/// Shared config-crate builder: base file → immutable environment overlay.
fn build_config(config_dir: &str) -> Result<config::Config, config::ConfigError> {
    config::Config::builder()
        .add_source(config::File::with_name(&format!("{config_dir}/quant-pivot")).required(false))
        .add_source(
            config::File::with_name(&format!("{config_dir}/quant-pivot.local")).required(false),
        )
        .build()
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
    use std::{
        env::{self, var},
        fs,
        path::{Path, PathBuf},
        process,
    };

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
    fn public_rpc_endpoint_rejects_secret_bearing_url_shapes() {
        let mut deploy = DeployConfig::default();
        deploy.polymarket.onchain.rpc_endpoint = PolygonRpcEndpoint::Public {
            url: "https://provider.invalid/v2/provider-key".to_owned(),
        };
        let error = deploy
            .ensure_valid_common()
            .expect_err("a provider key in a public URL path must fail closed");
        let message = error.to_string();
        assert!(message.contains("polymarket.onchain.rpc_endpoint"));
        assert!(!message.contains("provider-key"));
    }

    #[test]
    fn protected_rpc_endpoint_accepts_authenticated_url_without_debug_leakage() {
        let mut deploy = DeployConfig::default();
        deploy.polymarket.onchain.rpc_endpoint = PolygonRpcEndpoint::SystemdCredential {
            credential: secret::SystemdCredentialRef::from_resolved(
                "https://provider.invalid/v2/provider-key?tenant=private",
            ),
        };
        deploy
            .ensure_valid_common()
            .expect("resolved authenticated RPC endpoint must validate");
        assert!(!format!("{deploy:?}").contains("provider-key"));
    }

    #[test]
    fn report_only_requires_credentials() {
        // report_only is not dry-run: it reads the real venue account, so a
        // missing private key / funder fails closed in every mode.
        let deploy = DeployConfig::default();
        assert!(
            deploy
                .validate_for_quant_mode(QuantRuntimeMode::ReportOnly)
                .has_errors()
        );
    }

    #[test]
    fn report_only_validates_with_credentials() {
        let mut deploy = DeployConfig::default();
        deploy.keys.private_key = Some("0xabc".into());
        deploy.quant.account.funder = Some("0xfunder".into());
        deploy.web.jwt.signing_key = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc".into();
        deploy
            .ensure_valid_for_quant_mode(QuantRuntimeMode::ReportOnly)
            .expect("report_only with credentials must validate");
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
    fn workspace_config_dir() -> PathBuf {
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
        let template = config_dir.join("quant-pivot.toml");
        if !template.exists() {
            eprintln!("skipping shipped_toml_template_deserializes: template missing");
            return;
        }
        let raw = fs::read_to_string(template).expect("read shipped TOML");
        let deploy: DeployConfig = toml::from_str(&raw).expect("deserialize shipped TOML");
        deploy
            .ensure_valid_common()
            .expect("shipped TOML must validate structurally");
    }

    #[test]
    fn typed_credential_reference_deserializes_and_plaintext_is_rejected() {
        let typed: DeployConfig =
            toml::from_str("[keys]\nprivate_key = { name = \"polymarket-private-key\" }\n")
                .expect("typed credential reference");
        assert_eq!(
            typed
                .keys
                .private_key
                .as_ref()
                .map(|value| value.name.as_str()),
            Some("polymarket-private-key")
        );

        let plaintext: Result<DeployConfig, _> =
            toml::from_str("[keys]\nprivate_key = \"0xplaintext\"\n");
        assert!(
            plaintext.is_err(),
            "deploy config must never accept a plaintext credential"
        );
    }

    #[test]
    fn immutable_local_toml_overrides_non_secret_base_values() {
        let dir = env::temp_dir().join(format!("quant_pivot_local_cfg_test_{}", process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("temp config dir");

        fs::write(
            dir.join("quant-pivot.toml"),
            "[db.postgres]\nhost = \"base.internal\"\n",
        )
        .expect("write base");
        fs::write(
            dir.join("quant-pivot.local.toml"),
            "[db.postgres]\nhost = \"staging.internal\"\n",
        )
        .expect("write immutable environment override");

        let deploy = DeployConfig::load(dir.to_str().expect("utf-8")).expect("load");
        assert_eq!(deploy.db.postgres.host, "staging.internal");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn migration_credential_reference_uses_local_override_without_plaintext() {
        let dir = env::temp_dir().join(format!("quant_pivot_migration_cfg_{}", process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("temp config dir");
        fs::write(
            dir.join("quant-pivot.toml"),
            "[db.postgres.migration]\npassword = { name = \"pg-migration-base\" }\n",
        )
        .expect("write base config");
        fs::write(
            dir.join("quant-pivot.local.toml"),
            "[db.postgres.migration]\npassword = { name = \"pg-migration-staging\" }\n",
        )
        .expect("write environment override");

        let deploy: DeployConfig = build_config(dir.to_str().expect("utf-8"))
            .expect("build config")
            .try_deserialize()
            .expect("deserialize typed reference");
        assert_eq!(
            deploy
                .db
                .postgres
                .migration
                .password
                .as_ref()
                .map(|credential| credential.name.as_str()),
            Some("pg-migration-staging")
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn production_runtime_rejects_migration_password() {
        let dir = env::temp_dir().join(format!("quant_pivot_runtime_secret_{}", process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("temp config dir");
        fs::write(
            dir.join("quant-pivot.toml"),
            "[db.clickhouse]\ndeployment_id = \"production\"\n\
             [db.postgres.migration]\npassword = { name = \"postgres-migration-password\" }\n",
        )
        .expect("write production config");
        let error = DeployConfig::load(dir.to_str().expect("utf-8"))
            .expect_err("runtime must reject DDL credentials");
        assert!(
            error
                .to_string()
                .contains("must not contain DDL credentials")
        );
        assert!(!error.to_string().contains("postgres-migration-password"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn production_runtime_treats_empty_migration_password_as_absent() {
        let dir = env::temp_dir().join(format!("quant_pivot_empty_ddl_secret_{}", process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("temp config dir");
        fs::write(
            dir.join("quant-pivot.toml"),
            "[db.clickhouse]\ndeployment_id = \"production\"\n\
             [db.postgres.migration]\npassword = { name = \"\" }\n",
        )
        .expect("write production config");
        let deploy = DeployConfig::load(dir.to_str().expect("utf-8"))
            .expect("empty DDL password is not a credential");
        assert!(deploy.db.postgres.migration.password.is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn production_example_toml_deserializes_and_validates() {
        let template = workspace_config_dir().join("quant-pivot.production.example.toml");
        if !template.exists() {
            eprintln!("skipping production_example_toml_deserializes: {template:?} missing");
            return;
        }
        let raw = fs::read_to_string(&template).expect("read production example");
        let mut parsed: DeployConfig =
            toml::from_str(&raw).expect("production example must deserialize");
        assert_eq!(parsed.db.postgres.migration.user, "quant_pivot_migrator");
        assert_eq!(parsed.db.clickhouse.migration.user, "quant_pivot_migrator");
        assert!(matches!(
            &parsed.polymarket.onchain.rpc_endpoint,
            PolygonRpcEndpoint::SystemdCredential { credential }
                if credential.name == "polygon-rpc-url"
        ));
        parsed.polymarket.onchain.rpc_endpoint = PolygonRpcEndpoint::SystemdCredential {
            credential: secret::SystemdCredentialRef::from_resolved(
                "https://provider.invalid/v2/test-provider-key",
            ),
        };
        parsed
            .ensure_valid_common()
            .expect("production example must validate after documented secret injection");
    }
}
