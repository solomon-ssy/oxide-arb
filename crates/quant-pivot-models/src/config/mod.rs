//! Deploy configuration tree (`config/quant-pivot.toml`, restart to apply).
//!
//! [`DeployConfig`] owns everything that is structurally bound to a process
//! start: connection endpoints and pools, channel capacities and shard counts,
//! plaintext secrets, the web server, and logging. Operator tunables that must
//! change **without** a restart live in independently revisioned
//! [`DecisionPolicySnapshot`](crate::runtime_config::DecisionPolicySnapshot) instead and are
//! managed through the governed Config API — never by editing TOML.
//!
//! Loading accepts exactly one explicit absolute TOML file. There is no default
//! path, directory discovery, environment source, overlay, or default fill.

mod cache;
mod db;
mod deployment;
mod descriptor;
mod domain_sources;
mod keys;
mod market_data;
mod observability;
mod polymarket;
mod projection;
mod quant;
mod research;
pub mod secret;
pub mod validation;
mod validation_contract;
mod web;

pub use cache::{CacheConfig, DomainCacheConfig, MokaConfig, RedisConfig};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Take},
    path::PathBuf,
};

#[cfg(all(test, unix))]
use std::fs::Permissions;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

pub use db::{
    CLICKHOUSE_BULK_ACK_MAX_MS, CLICKHOUSE_CANONICAL_PUBLICATION_TIMEOUT_MS,
    CLICKHOUSE_CRITICAL_ATTEMPT_MAX_MS, CLICKHOUSE_DERIVED_FACT_FLUSH_MS,
    CLICKHOUSE_DURABLE_ACK_TIMEOUT_MS, CLICKHOUSE_DURABLE_ADMISSION_TIMEOUT_MS,
    CLICKHOUSE_DURABLE_SCHEDULING_MARGIN_MS, CLICKHOUSE_DURABLE_SHUTDOWN_STAGE_SECS,
    CLICKHOUSE_FLUSH_INTERVAL_MAX_SECS, CLICKHOUSE_INSERT_MAX_ATTEMPTS,
    CLICKHOUSE_INSERT_RETRY_BACKOFF_BASE_MS, CLICKHOUSE_INSERT_RETRY_BACKOFF_TOTAL_MS,
    ClickHouseConfig, ClickHouseInsertIoConfig, ClickHouseIoConfig, ClickHouseResourceGovernance,
    DatabaseConfig, PostgresConfig,
};
pub use deployment::DeploymentConfig;
pub use descriptor::{
    DEPLOY_CONFIG_EXPECTED_LEAF_COUNT, DEPLOY_SECRET_PATHS, DeployApplyEffect,
    DeployConfigDescriptor, DeployConfigFieldDescriptor, DeployFieldBounds, DeployFieldUnit,
    DeploySensitivity, DeployValueKind,
};
pub use domain_sources::{
    AirNowPm25ReportingAreaBindingConfig, AirNowPm25SiteBindingConfig, AirNowSourceConfig,
    AviationWeatherSourceConfig, BinanceSourceConfig, ChainlinkDataStreamFeedConfig,
    ChainlinkDataStreamsSourceConfig, DomainSourcesConfig, GefsSourceConfig, GhcndSourceConfig,
    GhcnhSourceConfig, HkoDailyTemperatureBindingConfig, HkoOpenDataSourceConfig,
    HkoRainfallBindingConfig, NasaGistempSourceConfig, NhcHistoricalStormBindingConfig,
    NhcSourceConfig, NsidcSeaIceSourceConfig, NwsObservationSourceConfig,
    NwsWindStationBindingConfig, PolymarketRtdsSourceConfig, TornadoRegionBindingConfig,
    TornadoRegionScopeConfig, TornadoSourceConfig, WEATHER_OBSERVATION_DAY_CLOSE_GRACE_SECS,
    WeatherHistoricalBindingKind, WeatherStationProfileConfig, WeatherVerticalBindingsConfig,
    builtin_weather_station_profiles,
};
pub use keys::KeysConfig;
pub use market_data::{
    DataApiConfig, EXCHANGE_HISTORY_MAX_BLOCKS_PER_CHUNK, EXCHANGE_HISTORY_MAX_BLOCKS_PER_TICK,
    ExchangeHistoryAttestorConfig, FinalizedExchangeHistoryConfig, GammaConfig, HyperSyncConfig,
    MarketDataDeployConfig, WebSocketConfig,
};
#[cfg(unix)]
use nix::{fcntl::OFlag, unistd::Uid};
pub use observability::{
    NotificationChannelsConfig, ObservabilityConfig, TelegramChannelConfig, WebhookChannelConfig,
};
pub use polymarket::{
    OnchainConfig, PolygonRpcEndpoint, PolymarketConfig, RelayerConfig, SettlementDeployConfig,
};
pub use projection::{
    DeployConfigFieldProjection, DeployProjectedValue, DeployProjectionError, DeployProtectedStatus,
};
pub use quant::{
    FeatureParityComputeConfig, FeedbackAttributionComputeConfig, PortfolioSolverDeployConfig,
    QuantAccountDeployConfig, QuantDeployConfig, QuantWorkersConfig, ResearchJobsConfig,
};
use quant_pivot_error::{
    QuantResult, config::ConfigError, config_validation::ConfigValidationReport,
};
pub use research::{
    ArtifactStoreDeployConfig, ArtifactStoreKind, EvidenceAttestationConfig,
    ModelServingRegistryConfig, ResearchDeployConfig,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};
use toml::{Value as TomlValue, de::Error as TomlDeError};
pub use validation_contract::DeployValidationRuleDescriptor;
pub use web::{JwtConfig, PasswordCryptoConfig, WebConfig};

use crate::types::DeploymentEnvironment;

const MAX_CONFIG_FILE_BYTES: u64 = 8 * 1024 * 1024;

/// Explicit, immutable request for one Deploy Config file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeployConfigLoadRequest {
    pub config_file: PathBuf,
    pub expected_environment: DeploymentEnvironment,
}

impl DeployConfigLoadRequest {
    #[must_use]
    pub const fn new(config_file: PathBuf, expected_environment: DeploymentEnvironment) -> Self {
        Self {
            config_file,
            expected_environment,
        }
    }
}

struct OpenedDeployConfig {
    raw: String,
    mode: u32,
}

/// Deserialized deploy-configuration root.
///
/// Each section maps 1:1 to a `[section]` in `quant-pivot.toml`. Wrap in an
/// `Arc` for sharing across async tasks — the struct itself is plain data.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeployConfig {
    /// Environment identity used for environment-specific operational safety.
    pub deployment: DeploymentConfig,
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

/// Explicit serialization capability for generated, redacted templates only.
///
/// `DeployConfig` deliberately does not implement `Serialize`, preventing an
/// API or log boundary from serializing the runtime object by accident. This
/// adapter retains the existing per-secret empty-value serializers and is
/// consumed only by the descriptor-owned template generator.
pub struct DeployConfigTemplate<'a> {
    config: &'a DeployConfig,
}

impl<'a> From<&'a DeployConfig> for DeployConfigTemplate<'a> {
    fn from(config: &'a DeployConfig) -> Self {
        Self { config }
    }
}

impl Serialize for DeployConfigTemplate<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut root = serializer.serialize_struct("DeployConfigTemplate", 12)?;
        root.serialize_field("deployment", &self.config.deployment)?;
        root.serialize_field("polymarket", &self.config.polymarket)?;
        root.serialize_field("market_data", &self.config.market_data)?;
        root.serialize_field("domain_sources", &self.config.domain_sources)?;
        root.serialize_field("observability", &self.config.observability)?;
        root.serialize_field("notifications", &self.config.notifications)?;
        root.serialize_field("db", &self.config.db)?;
        root.serialize_field("cache", &self.config.cache)?;
        root.serialize_field("keys", &self.config.keys)?;
        root.serialize_field("web", &self.config.web)?;
        root.serialize_field("quant", &self.config.quant)?;
        root.serialize_field("research", &self.config.research)?;
        root.end()
    }
}

impl DeployConfig {
    /// Load one explicit Deploy Config file through a no-follow file descriptor.
    pub fn load(request: &DeployConfigLoadRequest) -> QuantResult<Self> {
        Self::load_internal(request)
    }

    fn load_internal(request: &DeployConfigLoadRequest) -> QuantResult<Self> {
        let opened = OpenedDeployConfig::read(request)?;
        let parsed: TomlValue =
            toml::from_str(&opened.raw).map_err(|error| ConfigError::Parse {
                path: request.config_file.clone(),
                reason: error.message().to_owned(),
            })?;
        if Self::contains_placeholder(&parsed) {
            return Err(ConfigError::Placeholder {
                path: request.config_file.clone(),
            }
            .into());
        }
        let mut deploy: Self =
            parsed
                .try_into()
                .map_err(|error: TomlDeError| ConfigError::Parse {
                    path: request.config_file.clone(),
                    reason: error.message().to_owned(),
                })?;
        deploy.keys.normalize();
        deploy.polymarket.relayer.normalize();
        deploy
            .domain_sources
            .chainlink_data_streams
            .normalize_credentials();
        if deploy.deployment.environment != request.expected_environment {
            return Err(ConfigError::EnvironmentMismatch {
                expected: request.expected_environment.as_str().to_owned(),
                actual: deploy.deployment.environment.as_str().to_owned(),
            }
            .into());
        }
        OpenedDeployConfig::validate_mode(request, opened.mode, &deploy)?;
        deploy.ensure_valid_common()?;
        Ok(deploy)
    }

    fn contains_placeholder(value: &TomlValue) -> bool {
        match value {
            TomlValue::String(value) => value.contains("REPLACE_WITH_"),
            TomlValue::Array(values) => values.iter().any(Self::contains_placeholder),
            TomlValue::Table(values) => values.values().any(Self::contains_placeholder),
            _ => false,
        }
    }

    fn has_configured_secrets(&self) -> bool {
        let protected_rpc = matches!(
            &self.polymarket.onchain.rpc_endpoint,
            PolygonRpcEndpoint::Protected { url } if !url.is_empty()
        );
        let protected_attestor = matches!(
            &self
                .market_data
                .finalized_exchange_history
                .attestor
                .rpc_endpoint,
            PolygonRpcEndpoint::Protected { url } if !url.is_empty()
        );
        protected_rpc
            || protected_attestor
            || !self
                .market_data
                .finalized_exchange_history
                .hypersync
                .api_token
                .is_empty()
            || self
                .polymarket
                .relayer
                .api_key
                .as_ref()
                .is_some_and(|secret| !secret.is_empty())
            || self
                .domain_sources
                .chainlink_data_streams
                .api_key
                .as_ref()
                .is_some_and(|secret| !secret.is_empty())
            || self
                .domain_sources
                .chainlink_data_streams
                .api_secret
                .as_ref()
                .is_some_and(|secret| !secret.is_empty())
            || !self.notifications.telegram.bot_token.is_empty()
            || !self.notifications.webhook.url.is_empty()
            || !self.notifications.webhook.authorization.is_empty()
            || !self.db.postgres.password.is_empty()
            || !self.db.clickhouse.password.is_empty()
            || !self.cache.redis.password.is_empty()
            || self
                .keys
                .private_key
                .as_ref()
                .is_some_and(|secret| !secret.is_empty())
            || !self.web.jwt.signing_key.is_empty()
            || !self.research.evidence_attestation.signing_key.is_empty()
            || self
                .research
                .evidence_attestation
                .previous_signing_keys
                .iter()
                .any(|secret| !secret.is_empty())
    }
}

impl OpenedDeployConfig {
    fn read(request: &DeployConfigLoadRequest) -> QuantResult<Self> {
        if !request.config_file.is_absolute() {
            return Err(ConfigError::UnsafeFile {
                path: request.config_file.clone(),
                reason: "config_file must be an absolute path".to_owned(),
            }
            .into());
        }
        Self::read_platform(request)
    }

    #[cfg(unix)]
    fn read_platform(request: &DeployConfigLoadRequest) -> QuantResult<Self> {
        let flags = (OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK).bits();
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(flags)
            .open(&request.config_file)
            .map_err(|source| ConfigError::FileIo {
                path: request.config_file.clone(),
                source,
            })?;
        let metadata = file.metadata().map_err(|source| ConfigError::FileIo {
            path: request.config_file.clone(),
            source,
        })?;
        if !metadata.file_type().is_file() {
            return Err(ConfigError::UnsafeFile {
                path: request.config_file.clone(),
                reason: "config_file must be a regular file".to_owned(),
            }
            .into());
        }
        if metadata.uid() != Uid::effective().as_raw() {
            return Err(ConfigError::UnsafeFile {
                path: request.config_file.clone(),
                reason: "config_file must be owned by the runtime user".to_owned(),
            }
            .into());
        }
        if metadata.len() > MAX_CONFIG_FILE_BYTES {
            return Err(ConfigError::UnsafeFile {
                path: request.config_file.clone(),
                reason: format!("config_file exceeds {MAX_CONFIG_FILE_BYTES} bytes"),
            }
            .into());
        }
        let mode = metadata.mode() & 0o777;
        let mut raw = String::new();
        let mut bounded: Take<&mut File> = file.by_ref().take(MAX_CONFIG_FILE_BYTES + 1);
        bounded
            .read_to_string(&mut raw)
            .map_err(|source| ConfigError::FileIo {
                path: request.config_file.clone(),
                source,
            })?;
        Ok(Self { raw, mode })
    }

    #[cfg(not(unix))]
    fn read_platform(request: &DeployConfigLoadRequest) -> QuantResult<Self> {
        let _ = OpenOptions::new();
        Err(ConfigError::UnsafeFile {
            path: request.config_file.clone(),
            reason: "secure Deploy Config loading requires a Unix file descriptor".to_owned(),
        }
        .into())
    }

    fn validate_mode(
        request: &DeployConfigLoadRequest,
        mode: u32,
        deploy: &DeployConfig,
    ) -> QuantResult<()> {
        let production = request.expected_environment.as_str() == "production";
        let private_mode = matches!(mode, 0o400 | 0o600);
        if production && !private_mode {
            return Err(ConfigError::UnsafeFile {
                path: request.config_file.clone(),
                reason: format!(
                    "production config_file mode must be 0400 or 0600, found {mode:04o}"
                ),
            }
            .into());
        }
        if !production && deploy.has_configured_secrets() && !private_mode {
            return Err(ConfigError::UnsafeFile {
                path: request.config_file.clone(),
                reason: format!(
                    "a config_file containing secrets must be 0400 or 0600, found {mode:04o}"
                ),
            }
            .into());
        }
        if !production && !private_mode && mode != 0o644 {
            return Err(ConfigError::UnsafeFile {
                path: request.config_file.clone(),
                reason: format!(
                    "non-secret development config_file mode must be 0400, 0600, or 0644, found {mode:04o}"
                ),
            }
            .into());
        }
        Ok(())
    }
}

impl DeployConfig {
    /// Run deployment-invariant validation and fail closed on errors.
    ///
    /// Warnings are streamed to `tracing::warn` as a side effect so callers
    /// get uniform telemetry. Also used by [`Self::load`].
    pub fn ensure_valid_common(&self) -> QuantResult<()> {
        let report = self.validate_deploy_common();
        for w in &report.warnings {
            tracing::warn!("Deploy config warning: {w}");
        }
        if report.has_errors() {
            return Err(ConfigError::from(report).into());
        }
        Ok(())
    }

    /// Validate credentials and web authentication required by every execution-
    /// capable deployment, independently of runtime authorization state.
    #[must_use]
    pub fn validate_execution(&self) -> ConfigValidationReport {
        let report = self.execution_validation_report();
        for w in &report.warnings {
            tracing::warn!("Deploy config warning: {w}");
        }
        report
    }

    /// Fail-closed gate for execution credentials and authentication.
    pub fn ensure_execution_valid(&self) -> QuantResult<()> {
        let report = self.validate_execution();
        if report.has_errors() {
            return Err(ConfigError::from(report).into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env::var,
        fs,
        path::{Path, PathBuf},
    };

    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};

    use secret::SecretText;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn missing_explicit_file_fails() {
        let request = DeployConfigLoadRequest::new(
            PathBuf::from("/definitely/missing/quant-pivot.toml"),
            DeploymentEnvironment::local_development(),
        );
        assert!(DeployConfig::load(&request).is_err());
    }

    #[test]
    fn relative_file_is_rejected() {
        let request = DeployConfigLoadRequest::new(
            PathBuf::from("config/quant-pivot.toml"),
            DeploymentEnvironment::local_development(),
        );
        let error = DeployConfig::load(&request).expect_err("relative path must fail closed");
        assert!(error.to_string().contains("absolute path"));
    }

    #[test]
    fn defaults_validate_clean() {
        DeployConfig::default()
            .ensure_valid_common()
            .expect("defaults must validate");
    }

    #[test]
    fn public_rpc_rejects_shapes() {
        let mut deploy = DeployConfig::default();
        deploy.polymarket.onchain.rpc_endpoint = PolygonRpcEndpoint::Public {
            url: "https://provider.invalid/v2/provider-key".to_owned(),
        };
        let error = deploy
            .ensure_valid_common()
            .expect_err("a provider key in a public URL path must fail closed");
        let message = error.to_string();
        assert!(message.contains("polymarket.onchain"));
        assert!(!message.contains("provider-key"));
    }

    #[test]
    fn protected_accepts_without_leakage() {
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
    fn deploy_debug_redacts_family() {
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
    fn execution_requires_credentials() {
        // production account reads is not dry-run: it reads the real venue account, so a
        // missing private key / funder fails closed for every report and execution path.
        let deploy = DeployConfig::default();
        assert!(deploy.validate_execution().has_errors());
    }

    #[test]
    fn execution_validates_credentials() {
        let mut deploy = DeployConfig::default();
        deploy.keys.private_key = Some("0xabc".into());
        deploy.quant.account.funder = Some("0xfunder".into());
        deploy.web.jwt.signing_key = "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc".into();
        deploy
            .ensure_execution_valid()
            .expect("production account reads with credentials must validate");
    }

    #[test]
    fn ensure_fails_missing_credentials() {
        let deploy = DeployConfig::default();
        let err = deploy
            .ensure_execution_valid()
            .expect_err("policy-authorized execution without credentials must fail closed");
        assert!(err.to_string().contains("missing required credentials"));
    }

    #[test]
    fn unknown_section_is_rejected() {
        let toml = "[treasury]\ntarget_balance_usd = \"1000\"\n";
        let result: Result<DeployConfig, _> = toml::from_str(toml);
        assert!(result.is_err(), "stale [treasury] section must be fatal");
    }

    #[test]
    fn missing_leaf_is_rejected() {
        let raw = fs::read_to_string(tracked_config_root().join("quant-pivot.toml"))
            .expect("read shipped TOML");
        let mut value: TomlValue = toml::from_str(&raw).expect("parse shipped TOML value");
        value
            .get_mut("deployment")
            .and_then(TomlValue::as_table_mut)
            .expect("deployment table")
            .remove("environment");
        let missing_leaf = toml::to_string(&value).expect("serialize missing-leaf fixture");
        let result: Result<DeployConfig, _> = toml::from_str(&missing_leaf);
        assert!(
            result.is_err(),
            "every static Deploy Config leaf must be explicitly present"
        );
    }

    #[test]
    fn runtime_sections_rejected_toml() {
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
    fn tracked_config_root() -> PathBuf {
        let crate_dir = var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_owned());
        Path::new(&crate_dir)
            .ancestors()
            .nth(2)
            .expect("workspace root")
            .join("config")
    }

    #[test]
    fn shipped_toml_template_deserializes() {
        let config_root = tracked_config_root();
        let template = config_root.join("quant-pivot.toml");
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

    #[cfg(unix)]
    #[test]
    fn explicit_regular_file_loads() {
        let directory = tempdir().expect("temp config directory");
        let target = directory.path().join("runtime.toml");
        fs::copy(tracked_config_root().join("quant-pivot.toml"), &target)
            .expect("copy tracked config");
        fs::set_permissions(&target, Permissions::from_mode(0o644))
            .expect("set public template mode");
        let request =
            DeployConfigLoadRequest::new(target, DeploymentEnvironment::local_development());
        DeployConfig::load(&request).expect("explicit development template loads");
    }

    #[cfg(unix)]
    #[test]
    fn symlink_is_rejected() {
        let directory = tempdir().expect("temp config directory");
        let target = directory.path().join("target.toml");
        fs::copy(tracked_config_root().join("quant-pivot.toml"), &target)
            .expect("copy tracked config");
        let link = directory.path().join("linked.toml");
        symlink(&target, &link).expect("create symlink");
        let request =
            DeployConfigLoadRequest::new(link, DeploymentEnvironment::local_development());
        assert!(DeployConfig::load(&request).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn dangerous_mode_is_rejected() {
        let directory = tempdir().expect("temp config directory");
        let target = directory.path().join("runtime.toml");
        fs::copy(tracked_config_root().join("quant-pivot.toml"), &target)
            .expect("copy tracked config");
        fs::set_permissions(&target, Permissions::from_mode(0o666)).expect("set dangerous mode");
        let request =
            DeployConfigLoadRequest::new(target, DeploymentEnvironment::local_development());
        let error = DeployConfig::load(&request).expect_err("dangerous mode must fail closed");
        assert!(error.to_string().contains("mode must be"));
    }

    #[cfg(unix)]
    #[test]
    fn environment_mismatch_is_rejected() {
        let directory = tempdir().expect("temp config directory");
        let target = directory.path().join("runtime.toml");
        fs::copy(tracked_config_root().join("quant-pivot.toml"), &target)
            .expect("copy tracked config");
        fs::set_permissions(&target, Permissions::from_mode(0o600)).expect("set private mode");
        let request = DeployConfigLoadRequest::new(
            target,
            DeploymentEnvironment::parse("production").expect("production environment"),
        );
        let error = DeployConfig::load(&request).expect_err("environment mismatch must fail");
        assert!(error.to_string().contains("environment mismatch"));
    }

    #[cfg(unix)]
    #[test]
    fn placeholders_are_rejected() {
        let directory = tempdir().expect("temp config directory");
        let target = directory.path().join("runtime.toml");
        fs::copy(
            tracked_config_root().join("quant-pivot.production.example.toml"),
            &target,
        )
        .expect("copy production template");
        fs::set_permissions(&target, Permissions::from_mode(0o600)).expect("set private mode");
        let request = DeployConfigLoadRequest::new(
            target,
            DeploymentEnvironment::parse("production").expect("production environment"),
        );
        let error = DeployConfig::load(&request).expect_err("placeholder must fail closed");
        assert!(error.to_string().contains("unreplaced placeholder"));
    }
}
