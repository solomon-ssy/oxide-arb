//! Deploy configuration tree (`config/quant-pivot.toml`, restart to apply).
//!
//! [`DeployConfig`] owns everything that is structurally bound to a process
//! start: connection endpoints and pools, channel capacities and shard counts,
//! plaintext secrets, the web server, and logging. Operator tunables that must
//! change **without** a restart live in independently revisioned
//! [`DecisionPolicySnapshot`](crate::runtime_config::DecisionPolicySnapshot) instead and are
//! managed through the governed Config API — never by editing TOML.
//!
//! # Loading precedence (high → low)
//!
//! The `config` crate merges sources in registration order; **later wins**:
//!
//! 1. Optional gitignored machine-local TOML override
//! 2. `config/quant-pivot.toml`
//! 3. Hard-coded defaults (`serde` `default` on each struct field)
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

pub use cache::{CacheConfig, DomainCacheConfig, MokaConfig, RedisConfig};
use config_rs::{Config, ConfigError as ConfigConfigError, File};
pub use db::{ClickHouseConfig, DatabaseConfig, PostgresConfig};
pub use domain_sources::{
    AirNowPm25ReportingAreaBindingConfig, AirNowPm25SiteBindingConfig, AirNowSourceConfig,
    AviationWeatherSourceConfig, BinanceSourceConfig, ChainlinkDataStreamFeedConfig,
    ChainlinkDataStreamsSourceConfig, DomainSourcesConfig, GefsSourceConfig, GhcnhSourceConfig,
    HkoDailyTemperatureBindingConfig, HkoOpenDataSourceConfig, HkoRainfallBindingConfig,
    NasaGistempSourceConfig, NhcHistoricalStormBindingConfig, NhcSourceConfig,
    NsidcSeaIceSourceConfig, NwsObservationSourceConfig, NwsWindStationBindingConfig,
    PolymarketRtdsSourceConfig, TornadoRegionBindingConfig, TornadoSourceConfig,
    WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS, WeatherHistoricalBindingKind,
    WeatherStationProfileConfig, WeatherVerticalBindingsConfig, builtin_weather_station_profiles,
};
pub use keys::KeysConfig;
pub use lifecycle::{CompiledBuildIdentity, LifecycleDeployConfig, ProjectLifecyclePolicy};
pub use market_data::{
    DataApiConfig, GammaConfig, MAX_TRADE_TAPE_RECONCILIATION_ROWS, MarketDataDeployConfig,
    TradeTapeOnChainConfig, WebSocketConfig,
};
pub use observability::{
    NotificationChannelsConfig, ObservabilityConfig, TelegramChannelConfig, WebhookChannelConfig,
};
pub use polymarket::{OnchainConfig, PolygonRpcEndpoint, PolymarketConfig, RelayerConfig};
pub use quant::{
    QuantAccountDeployConfig, QuantDeployConfig, QuantWorkersConfig, ResearchJobsConfig,
};
use quant_pivot_error::{
    QuantResult, config::ConfigError, config_validation::ConfigValidationReport,
};
pub use research::{
    ArtifactStoreDeployConfig, ArtifactStoreKind, EvidenceAttestationConfig, ResearchDeployConfig,
};
use serde::Deserialize;
pub use web::{JwtConfig, WebConfig};

use crate::{
    config::validation::{validate_deploy_common, validate_deploy_for_quant_mode},
    enums::quant::QuantRuntimeMode,
};

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
    /// Telegram/webhook bindings and secrets.
    pub notifications: NotificationChannelsConfig,
    /// Postgres + `ClickHouse` connections and write batching.
    pub db: DatabaseConfig,
    /// Redis + in-process cache layer.
    pub cache: CacheConfig,
    /// Wallet secret and identity binding.
    pub keys: KeysConfig,
    /// HTTP/WebSocket server + JWT.
    pub web: WebConfig,
    /// Quant pivot structural parameters and worker budgets.
    pub quant: QuantDeployConfig,
    /// Research plane settings (artifact-store root).
    pub research: ResearchDeployConfig,
}

impl DeployConfig {
    /// Load configuration from the given directory.
    ///
    /// Loads `{dir}/quant-pivot.toml` and an optional immutable environment
    /// override, then runs mode-agnostic semantic validation.
    pub fn load(config_dir: &str) -> QuantResult<Self> {
        Self::load_internal(config_dir)
    }

    /// Load the same deploy configuration for explicit schema commands.
    pub fn load_for_migration(config_dir: &str) -> QuantResult<Self> {
        Self::load_internal(config_dir)
    }

    fn load_internal(config_dir: &str) -> QuantResult<Self> {
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
        deploy.ensure_valid_common()?;
        deploy.lifecycle.validate_source_contract()?;
        Ok(deploy)
    }
}

/// Shared config-crate builder: base file → immutable environment overlay.
fn build_config(config_dir: &str) -> Result<Config, ConfigConfigError> {
    Config::builder()
        .add_source(File::with_name(&format!("{config_dir}/quant-pivot")).required(false))
        .add_source(File::with_name(&format!("{config_dir}/quant-pivot.local")).required(false))
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
    use std::{
        env::{self, var},
        fs,
        path::{Path, PathBuf},
        process,
    };

    use secret::SecretText;

    use super::*;

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
        deploy.polymarket.onchain.rpc_endpoint = PolygonRpcEndpoint::Protected {
            url: SecretText::from("https://provider.invalid/v2/provider-key?tenant=private"),
        };
        deploy
            .ensure_valid_common()
            .expect("resolved authenticated RPC endpoint must validate");
        assert!(!format!("{deploy:?}").contains("provider-key"));
    }

    #[test]
    fn deploy_debug_redacts_every_credential_family() {
        let sentinel = "qp-secret-redaction-sentinel";
        let mut deploy = DeployConfig::default();
        deploy.polymarket.onchain.rpc_endpoint = PolygonRpcEndpoint::Protected {
            url: format!("https://provider.invalid/{sentinel}").into(),
        };
        deploy.polymarket.relayer.api_key = Some(sentinel.into());
        deploy.domain_sources.chainlink_data_streams.api_key = Some(sentinel.into());
        deploy.domain_sources.chainlink_data_streams.api_secret = Some(sentinel.into());
        deploy.notifications.telegram.bot_token = sentinel.into();
        deploy.notifications.webhook.url = format!("https://alerts.invalid/{sentinel}").into();
        deploy.notifications.webhook.authorization = sentinel.into();
        deploy.db.postgres.password = sentinel.into();
        deploy.db.clickhouse.password = sentinel.into();
        deploy.cache.redis.password = sentinel.into();
        deploy.keys.private_key = Some(sentinel.into());
        deploy.web.jwt.signing_key = sentinel.into();
        deploy.research.evidence_attestation.signing_key = sentinel.into();
        deploy.research.evidence_attestation.previous_signing_keys =
            vec![sentinel.into(), sentinel.into()];

        let debug = format!("{deploy:?}");
        assert!(!debug.contains(sentinel));
        assert_eq!(debug.matches("<secret:redacted>").count(), 15);
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
    fn secret_text_deserializes_plaintext_and_stays_redacted() {
        let plaintext: DeployConfig =
            toml::from_str("[keys]\nprivate_key = \"0xplaintext\"\n").expect("plaintext secret");
        assert_eq!(plaintext.keys.private_key(), Some("0xplaintext"));
        assert!(!format!("{plaintext:?}").contains("0xplaintext"));
    }

    #[test]
    fn local_toml_overrides_base_values() {
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
        .expect("write machine-local override");

        let deploy = DeployConfig::load(dir.to_str().expect("utf-8")).expect("load");
        assert_eq!(deploy.db.postgres.host, "staging.internal");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn local_plaintext_overlay_reaches_postgres_adapter_and_stays_redacted() {
        let dir = env::temp_dir().join(format!("quant_pivot_local_secret_{}", process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("temp config dir");
        fs::write(dir.join("quant-pivot.toml"), "").expect("write base config");
        fs::write(
            dir.join("quant-pivot.local.toml"),
            "[db.postgres]\npassword = \"local-postgres-password\"\n",
        )
        .expect("write local secret override");

        let deploy = DeployConfig::load(dir.to_str().expect("utf-8"))
            .expect("load exact local-development plaintext");
        let url = deploy
            .db
            .postgres
            .try_connection_url()
            .expect("build PostgreSQL adapter URL");
        assert_eq!(
            url::Url::parse(&url)
                .expect("parse PostgreSQL URL")
                .password(),
            Some("local-postgres-password")
        );
        assert!(!format!("{deploy:?}").contains("local-postgres-password"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn production_example_deserializes_and_placeholders_fail_validation() {
        let template = workspace_config_dir().join("quant-pivot.production.example.toml");
        if !template.exists() {
            eprintln!("skipping production_example_toml_deserializes: {template:?} missing");
            return;
        }
        let raw = fs::read_to_string(&template).expect("read production example");
        let parsed: DeployConfig =
            toml::from_str(&raw).expect("production example must deserialize");
        assert!(matches!(
            &parsed.polymarket.onchain.rpc_endpoint,
            PolygonRpcEndpoint::Protected { url }
                if url.expose_secret().contains("REPLACE_WITH_")
        ));
        assert!(
            parsed
                .ensure_valid_for_quant_mode(QuantRuntimeMode::ReportOnly)
                .is_err(),
            "production placeholders must be replaced before startup"
        );
    }
}
