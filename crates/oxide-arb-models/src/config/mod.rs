//! Application configuration tree.
//!
//! The config module is split into domain-specific sub-modules for clarity.
//! [`Settings`] wraps the deserialized [`Inner`] in an `Arc` for cheap cloning
//! across async tasks and retains the `config_path` for runtime persistence.
//!
//! # Loading precedence (high → low)
//!
//! 1. Environment variables (`OXIDE_ARB__*`)
//! 2. `config/oxide-arb.toml`
//! 3. Hard-coded defaults via `#[serde(default)]`

mod analytics;
mod cache;
mod db;
mod detection;
mod execution;
mod fees;
mod keys;
mod market_data;
mod notification;
mod observability;
mod polymarket;
mod risk;
pub mod settlement;
mod treasury;
pub mod validation;

pub use analytics::*;
pub use cache::*;
pub use db::*;
pub use detection::*;
pub use execution::*;
pub use fees::*;
pub use keys::*;
pub use market_data::*;
pub use notification::*;
pub use observability::*;
pub use polymarket::*;
pub use risk::*;
pub use settlement::*;
pub use treasury::*;

use crate::{
    config::validation::{validate_settings_common, validate_settings_mode},
    enums::common::ExecutionMode,
};
use oxide_arb_error::{
    OxideResult, config::ConfigError, config_validation::ConfigValidationReport,
};
use serde::Deserialize;
use std::{ops::Deref, path::PathBuf, sync::Arc};

/// Top-level application settings.
///
/// Wraps the deserialized [`Inner`] in an `Arc` for cheap cloning across
/// async tasks. Runtime parameter adjustments (risk, detection) are handled
/// via the Web API (`PATCH /api/v1/config/risk`), not by reloading this file.
#[derive(Debug, Clone)]
pub struct Settings {
    inner: Arc<Inner>,
    config_path: Arc<PathBuf>,
}

impl Deref for Settings {
    type Target = Inner;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl Settings {
    /// Load configuration from the given directory.
    ///
    /// Loads `{dir}/oxide-arb.toml` and merges `OXIDE_ARB__*` environment
    /// variables on top. Runs semantic validation before returning — fatal
    /// errors cause an immediate `Err`, warnings are logged via `tracing`.
    pub fn new(config_dir: &str) -> OxideResult<Self> {
        let config_path = PathBuf::from(config_dir);
        let builder = config::Config::builder()
            .add_source(config::File::with_name(&format!("{config_dir}/oxide-arb")).required(false))
            .add_source(
                config::Environment::with_prefix("OXIDE_ARB")
                    .separator("__")
                    .try_parsing(true),
            );

        let inner: Inner = builder
            .build()
            .map_err(ConfigError::Load)?
            .try_deserialize()
            .map_err(ConfigError::Load)?;

        run_common_validation(&inner)?;

        Ok(Self {
            inner: Arc::new(inner),
            config_path: Arc::new(config_path),
        })
    }

    /// Convenience loader using the default `config/` directory.
    #[inline]
    pub fn load() -> OxideResult<Self> {
        Self::new("config")
    }

    /// Returns the config directory path used during initialization.
    #[inline]
    pub fn config_path(&self) -> &PathBuf {
        &self.config_path
    }

    /// Clone the deserialized configuration root for programmatic overrides
    /// (tests, harnesses) without re-parsing TOML.
    #[must_use]
    pub fn clone_inner(&self) -> Inner {
        self.inner.as_ref().clone()
    }

    /// Construct settings from a fully assembled [`Inner`].
    ///
    /// Runs the same semantic validation as [`Settings::new`]. Use this when
    /// building configuration in code (for example in integration tests).
    pub fn from_parts(inner: Inner, config_path: PathBuf) -> OxideResult<Self> {
        run_common_validation(&inner)?;

        Ok(Self {
            inner: Arc::new(inner),
            config_path: Arc::new(config_path),
        })
    }

    /// Run the **mode-aware** portion of configuration validation.
    ///
    /// [`Settings::new`] and [`Settings::from_parts`] already execute the
    /// mode-agnostic checks at load time. This method is called by the
    /// CLI runner once the final [`ExecutionMode`] has been determined
    /// (CLI subcommand overrides the TOML default), so that credential
    /// policy and any future mode-sensitive invariants are evaluated
    /// against the mode that will actually run.
    ///
    /// Warnings are logged via `tracing::warn`. Call [`Self::ensure_valid_for_mode`]
    /// at startup to fail closed on errors.
    #[must_use]
    pub fn validate_for_mode(&self, mode: ExecutionMode) -> ConfigValidationReport {
        let report = validate_settings_mode(self.inner.as_ref(), mode);
        for w in &report.warnings {
            tracing::warn!(mode = ?mode, "Config warning: {w}");
        }
        report
    }

    /// Fail-closed gate for mode-aware validation (mirrors [`run_common_validation`]).
    ///
    /// Live/Paper credential policy and other mode-sensitive invariants must pass
    /// before subsystems connect to PG/CLOB. Warnings are logged; only errors abort.
    pub fn ensure_valid_for_mode(&self, mode: ExecutionMode) -> OxideResult<()> {
        let report = self.validate_for_mode(mode);
        if report.has_errors() {
            return Err(ConfigError::from(report).into());
        }
        Ok(())
    }
}

/// Helper: run mode-agnostic validation and convert any fatal errors
/// into a bail-worthy [`OxideError`]. Warnings are streamed to
/// `tracing::warn` as a side effect so callers get uniform telemetry.
fn run_common_validation(inner: &Inner) -> OxideResult<()> {
    let report = validate_settings_common(inner);
    for w in &report.warnings {
        tracing::warn!("Config warning: {w}");
    }
    if report.has_errors() {
        return Err(ConfigError::from(report).into());
    }
    Ok(())
}

/// Deserialized configuration root.
///
/// Each section maps 1:1 to a `[section]` in `oxide-arb.toml`.
/// All fields carry `#[serde(default)]` so partial configs are always valid.
///
/// Single-strategy (endgame) + single-platform (polymarket) design.
/// See ADR-001 for rationale.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Inner {
    #[serde(default)]
    pub polymarket: PolymarketConfig,
    #[serde(default)]
    pub detection: DetectionConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub risk: RiskConfig,
    #[serde(default)]
    pub market_data: MarketDataConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub db: DatabaseConfig,
    #[serde(default)]
    pub analytics: AnalyticsConfig,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub treasury: TreasuryConfig,
    #[serde(default)]
    pub keys: KeysConfig,
    #[serde(default)]
    pub notification: NotificationConfig,
    #[serde(default)]
    pub settlement: SettlementConfig,
}

impl Inner {
    /// Minimum net profit (USD) required to act on an opportunity.
    ///
    /// Authoritative field: `[detection].min_profit_threshold_usd` (ADR-001).
    #[inline]
    pub const fn min_profit_threshold_usd(&self) -> rust_decimal::Decimal {
        self.detection.min_profit_threshold_usd
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{env::var, path::Path};

    #[test]
    fn default_config_deserializes() {
        let settings = Settings::new("nonexistent_dir_for_test");
        assert!(
            settings.is_ok(),
            "Default config should deserialize: {settings:?}"
        );
    }

    #[test]
    fn settings_deref_to_inner() {
        let settings = Settings::new("nonexistent_dir_for_test").expect("should load defaults");
        let _: &Inner = &settings;
        assert_eq!(settings.market_data.staleness_fresh_ms, 2_000);
    }

    #[test]
    fn from_parts_validates() {
        let inner = Inner::default();
        let result = Settings::from_parts(inner, PathBuf::from("test"));
        assert!(result.is_ok());
    }

    #[test]
    fn validate_for_mode_dry_run_permissive() {
        let settings = Settings::new("nonexistent_dir_for_test").expect("defaults");
        let report = settings.validate_for_mode(ExecutionMode::DryRun);
        assert!(!report.has_errors());
    }

    #[test]
    fn ensure_valid_for_mode_fails_on_live_missing_credentials() {
        let settings = Settings::new("nonexistent_dir_for_test").expect("defaults");
        let err = settings
            .ensure_valid_for_mode(ExecutionMode::Live)
            .expect_err("Live without credentials must fail closed");
        assert!(err.to_string().contains("missing required credentials"));
    }

    #[test]
    fn ensure_valid_for_mode_passes_dry_run() {
        let settings = Settings::new("nonexistent_dir_for_test").expect("defaults");
        settings
            .ensure_valid_for_mode(ExecutionMode::DryRun)
            .expect("DryRun defaults should pass mode validation");
    }

    #[test]
    fn min_profit_threshold_single_source_on_defaults() {
        let inner = Inner::default();
        assert_eq!(
            inner.min_profit_threshold_usd(),
            inner.detection.min_profit_threshold_usd
        );
    }

    #[test]
    fn shipped_toml_template_deserializes() {
        let crate_dir = var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_owned());
        let workspace_root = Path::new(&crate_dir)
            .ancestors()
            .nth(2)
            .expect("workspace root");
        let config_dir = workspace_root.join("config");
        let template = config_dir.join("oxide-arb.toml");
        if !template.exists() {
            eprintln!("skipping shipped_toml_template_deserializes: {template:?} missing");
            return;
        }
        let dir_str = config_dir.to_str().expect("utf-8");
        let settings = Settings::new(dir_str);
        assert!(
            settings.is_ok(),
            "config/oxide-arb.toml failed to deserialize: {settings:?}"
        );
    }
}
